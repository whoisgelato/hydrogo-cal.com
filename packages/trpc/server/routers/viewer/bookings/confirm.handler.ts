import { Prisma } from "@prisma/client";

import type { LocationObject } from "@calcom/app-store/locations";
import { getLocationValueForDB } from "@calcom/app-store/locations";
import { sendDeclinedEmailsAndSMS } from "@calcom/emails";
import { getAllCredentialsIncludeServiceAccountKey } from "@calcom/features/bookings/lib/getAllCredentialsForUsersOnEvent/getAllCredentials";
import { getCalEventResponses } from "@calcom/features/bookings/lib/getCalEventResponses";
import { handleConfirmation } from "@calcom/features/bookings/lib/handleConfirmation";
import { handleWebhookTrigger } from "@calcom/features/bookings/lib/handleWebhookTrigger";
import { workflowSelect } from "@calcom/features/ee/workflows/lib/getAllWorkflows";
import type { GetSubscriberOptions } from "@calcom/features/webhooks/lib/getWebhooks";
import type { EventPayloadType, EventTypeInfo } from "@calcom/features/webhooks/lib/sendPayload";
import { getBookerBaseUrl } from "@calcom/lib/getBookerUrl/server";
import getOrgIdFromMemberOrTeamId from "@calcom/lib/getOrgIdFromMemberOrTeamId";
import { getTeamIdFromEventType } from "@calcom/lib/getTeamIdFromEventType";
import { isPrismaObjOrUndefined } from "@calcom/lib/isPrismaObj";
import { parseRecurringEvent } from "@calcom/lib/isRecurringEvent";
import { processPaymentRefund } from "@calcom/lib/payment/processPaymentRefund";
import { getUsersCredentialsIncludeServiceAccountKey } from "@calcom/lib/server/getUsersCredentials";
import { getTranslation } from "@calcom/lib/server/i18n";
import { getTimeFormatStringFromUserTimeFormat } from "@calcom/lib/timeFormat";
import prisma from "@calcom/prisma";
import {
  BookingStatus,
  MembershipRole,
  WebhookTriggerEvents,
  UserPermissionRole,
} from "@calcom/prisma/enums";
import type { EventTypeMetadata } from "@calcom/prisma/zod-utils";
import type { CalendarEvent } from "@calcom/types/Calendar";

import { TRPCError } from "@trpc/server";

import type { TrpcSessionUser } from "../../../types";
import type { TConfirmInputSchema } from "./confirm.schema";

type ConfirmOptions = {
  ctx: {
    user: Pick<
      NonNullable<TrpcSessionUser>,
      "id" | "email" | "username" | "role" | "destinationCalendar"
    >;
  };
  input: TConfirmInputSchema;
};

export const confirmHandler = async ({ ctx, input }: ConfirmOptions) => {
  const { user } = ctx;
  const { bookingId, recurringEventId, reason: rejectionReason, confirmed, emailsEnabled, platformClientParams } =
    input;

  // Load booking (keep original select to avoid type changes)
  const booking = await prisma.booking.findUniqueOrThrow({
    where: { id: bookingId },
    select: {
      title: true,
      description: true,
      customInputs: true,
      startTime: true,
      endTime: true,
      attendees: true,
      eventTypeId: true,
      responses: true,
      metadata: true,
      userPrimaryEmail: true,
      eventType: {
        select: {
          id: true,
          owner: true,
          teamId: true,
          recurringEvent: true,
          title: true,
          slug: true,
          requiresConfirmation: true,
          currency: true,
          length: true,
          description: true,
          price: true,
          bookingFields: true,
          hideOrganizerEmail: true,
          disableGuests: true,
          customReplyToEmail: true,
          metadata: true,
          locations: true,
          team: { select: { id: true, name: true, parentId: true } },
          workflows: { select: { workflow: { select: workflowSelect } } },
          customInputs: true,
          parentId: true,
          parent: { select: { teamId: true } },
        },
      },
      location: true,
      userId: true,
      user: {
        select: {
          id: true,
          username: true,
          email: true,
          timeZone: true,
          timeFormat: true,
          name: true,
          destinationCalendar: true,
          locale: true,
        },
      },
      id: true,
      uid: true,
      payment: true,
      destinationCalendar: true,
      paid: true,
      recurringEventId: true,
      status: true,
      smsReminderNumber: true,
    },
  });

  await checkIfUserIsAuthorizedToConfirmBooking({
    eventTypeId: booking.eventTypeId,
    loggedInUserId: user.id,
    teamId: booking.eventType?.teamId || booking.eventType?.parent?.teamId,
    bookingUserId: booking.userId,
    userRole: user.role,
  });

  if (booking.status === BookingStatus.ACCEPTED) {
    throw new TRPCError({ code: "BAD_REQUEST", message: "Booking already confirmed" });
  }

  // If booking requires payment and is not paid, we don't allow confirmation (original behavior keeps ACCEPTED)
  if (confirmed && booking.payment.length > 0 && !booking.paid) {
    await prisma.booking.update({ where: { id: bookingId }, data: { status: BookingStatus.ACCEPTED } });
    return { message: "Booking confirmed", status: BookingStatus.ACCEPTED };
  }

  // ---------- FAST-ACK (always) ----------
  if (confirmed) {
    // 1) make status visible immediately
    if (booking.status !== BookingStatus.ACCEPTED) {
      await prisma.booking.update({ where: { id: bookingId }, data: { status: BookingStatus.ACCEPTED } });
    }

    // 2) run heavy confirmation work in background
    const kickoff = async () => {
      try {
        await processConfirmedAsync({
          bookingId,
          recurringEventId,
          user,
          emailsEnabled,
          platformClientParams,
        });
      } catch (e) {
        console.error("[post-booking async] handleConfirmation failed", e);
      }
    };
    if (typeof setImmediate !== "undefined") setImmediate(kickoff);
    else if (typeof queueMicrotask !== "undefined") queueMicrotask(kickoff);
    else void kickoff();

    // 3) return now
    return { message: "Booking confirmed", status: BookingStatus.ACCEPTED };
  } else {
    // REJECT path: flip status first
    await prisma.booking.update({
      where: { id: bookingId },
      data: { status: BookingStatus.REJECTED, rejectionReason },
    });

    // Background: refunds + decline notifications + webhooks
    const kickoffReject = async () => {
      try {
        await processRejectedAsync({
          bookingId,
          rejectionReason,
          emailsEnabled,
          platformClientParams,
        });
      } catch (e) {
        console.error("[post-booking async] rejection notifications failed", e);
      }
    };
    if (typeof setImmediate !== "undefined") setImmediate(kickoffReject);
    else if (typeof queueMicrotask !== "undefined") queueMicrotask(kickoffReject);
    else void kickoffReject();

    return { message: "Booking rejected", status: BookingStatus.REJECTED };
  }
};

// === Background workers (original heavy logic moved here) ===

async function processConfirmedAsync({
  bookingId,
  recurringEventId,
  user,
  emailsEnabled,
  platformClientParams,
}: {
  bookingId: number;
  recurringEventId?: number | null;
  user: ConfirmOptions["ctx"]["user"];
  emailsEnabled?: boolean | null;
  platformClientParams?: Record<string, unknown> | null;
}) {
  const booking = await prisma.booking.findUniqueOrThrow({
    where: { id: bookingId },
    select: {
      title: true,
      description: true,
      customInputs: true,
      startTime: true,
      endTime: true,
      attendees: true,
      eventTypeId: true,
      responses: true,
      metadata: true,
      userPrimaryEmail: true,
      eventType: {
        select: {
          id: true,
          owner: true,
          teamId: true,
          recurringEvent: true,
          title: true,
          slug: true,
          requiresConfirmation: true,
          currency: true,
          length: true,
          description: true,
          price: true,
          bookingFields: true,
          hideOrganizerEmail: true,
          disableGuests: true,
          customReplyToEmail: true,
          metadata: true,
          locations: true,
          team: { select: { id: true, name: true, parentId: true } },
          workflows: { select: { workflow: { select: workflowSelect } } },
          customInputs: true,
          parentId: true,
          parent: { select: { teamId: true } },
        },
      },
      location: true,
      userId: true,
      user: {
        select: {
          id: true,
          username: true,
          email: true,
          timeZone: true,
          timeFormat: true,
          name: true,
          destinationCalendar: true,
          locale: true,
        },
      },
      id: true,
      uid: true,
      payment: true,
      destinationCalendar: true,
      paid: true,
      recurringEventId: true,
      status: true,
      smsReminderNumber: true,
    },
  });

  // translations for attendees
  const translations = new Map<string, any>();
  const attendeesList = await Promise.all(
    booking.attendees.map(async (attendee) => {
      const locale = attendee.locale ?? "en";
      let translate = translations.get(locale);
      if (!translate) {
        translate = await getTranslation(locale, "common");
        translations.set(locale, translate);
      }
      return {
        name: attendee.name,
        email: attendee.email,
        timeZone: attendee.timeZone,
        phoneNumber: attendee.phoneNumber,
        language: { translate, locale },
      };
    })
  );

  const organizerOrganizationProfile = await prisma.profile.findFirst({
    where: { userId: user.id },
  });
  const organizerOrganizationId = organizerOrganizationProfile?.organizationId;
  const bookerUrl = await getBookerBaseUrl(booking.eventType?.team?.parentId ?? organizerOrganizationId ?? null);
  const tOrganizer = await getTranslation(booking.user?.locale ?? "en", "common");

  const evt: CalendarEvent = {
    type: booking?.eventType?.slug as string,
    title: booking.title,
    description: booking.description,
    bookerUrl,
    ...getCalEventResponses({
      bookingFields: booking.eventType?.bookingFields ?? null,
      booking,
    } as any),
    customInputs: isPrismaObjOrUndefined(booking.customInputs),
    startTime: booking.startTime.toISOString(),
    endTime: booking.endTime.toISOString(),
    organizer: {
      id: booking.user?.id,
      email: booking?.userPrimaryEmail || booking.user?.email || "Email-less",
      name: booking.user?.name || "Nameless",
      username: booking.user?.username || undefined,
      timeZone: booking.user?.timeZone || "Europe/London",
      timeFormat: getTimeFormatStringFromUserTimeFormat(booking.user?.timeFormat),
      language: { translate: tOrganizer, locale: booking.user?.locale ?? "en" },
    },
    attendees: attendeesList,
    location: booking.location ?? "",
    uid: booking.uid,
    destinationCalendar: booking.destinationCalendar
      ? [booking.destinationCalendar]
      : booking.user?.destinationCalendar
      ? [booking.user?.destinationCalendar]
      : [],
    requiresConfirmation: booking?.eventType?.requiresConfirmation ?? false,
    hideOrganizerEmail: booking.eventType?.hideOrganizerEmail,
    eventTypeId: booking.eventType?.id,
    customReplyToEmail: booking.eventType?.customReplyToEmail,
    team: !!booking.eventType?.team
      ? { name: booking.eventType.team.name, id: booking.eventType.team.id, members: [] }
      : undefined,
    ...(platformClientParams ? platformClientParams : {}),
  };

  const recurring = parseRecurringEvent(booking.eventType?.recurringEvent);
  if (recurring) {
    const groupedRecurringBookings = await prisma.booking.groupBy({
      where: { recurringEventId: booking.recurringEventId },
      by: [Prisma.BookingScalarFieldEnum.recurringEventId],
      _count: true,
    });
    recurring.count = groupedRecurringBookings[0]?._count ?? recurring.count;
    (evt as any).recurringEvent = parseRecurringEvent(recurring);
  }

  // credentials & conferencing
  const credentials = await getUsersCredentialsIncludeServiceAccountKey(user);
  const allCredentials = await getAllCredentialsIncludeServiceAccountKey(
    { ...user, credentials },
    { ...booking.eventType, metadata: booking.eventType?.metadata as EventTypeMetadata }
  );
  const conferenceCredentialId = getLocationValueForDB(
    booking.location ?? "",
    (booking.eventType?.locations as LocationObject[]) || []
  );
  (evt as any).conferenceCredentialId = conferenceCredentialId.conferenceCredentialId;

  // original heavy flow
  await handleConfirmation({
    user: { ...user, credentials: allCredentials },
    evt,
    recurringEventId: recurringEventId ?? booking.recurringEventId ?? undefined,
    prisma,
    bookingId: booking.id,
    booking,
    emailsEnabled,
    platformClientParams,
  });
}

async function processRejectedAsync({
  bookingId,
  rejectionReason,
  emailsEnabled,
  platformClientParams,
}: {
  bookingId: number;
  rejectionReason?: string | null;
  emailsEnabled?: boolean | null;
  platformClientParams?: Record<string, unknown> | null;
}) {
  const booking = await prisma.booking.findUniqueOrThrow({
    where: { id: bookingId },
    select: {
      eventTypeId: true,
      eventType: {
        select: {
          id: true,
          title: true,
          description: true,
          requiresConfirmation: true,
          price: true,
          currency: true,
          length: true,
          teamId: true,
          parentId: true,
          metadata: true,
        },
      },
      userId: true,
      smsReminderNumber: true,
      attendees: true,
      startTime: true,
      endTime: true,
      user: { select: { locale: true, name: true, email: true, timeFormat: true, timeZone: true } },
      location: true,
      title: true,
      description: true,
      responses: true,
      customInputs: true,
      destinationCalendar: true,
    },
  });

  // refunds if any
  if (!!(await prisma.payment.count({ where: { bookingId } }))) {
    await processPaymentRefund({ booking: booking as any, teamId: booking.eventType?.teamId });
  }

  // emails
  if (emailsEnabled) {
    const tOrganizer = await getTranslation(booking.user?.locale ?? "en", "common");
    const attendeesList = booking.attendees.map((a) => ({
      name: a.name,
      email: a.email,
      timeZone: a.timeZone,
      phoneNumber: a.phoneNumber,
      language: { translate: tOrganizer, locale: booking.user?.locale ?? "en" },
    }));
    const bookerUrl = await getBookerBaseUrl(booking.eventType?.teamId ?? null);
    const evt: any = {
      type: "",
      title: booking.title,
      description: booking.description,
      bookerUrl,
      attendees: attendeesList,
      startTime: booking.startTime.toISOString(),
      endTime: booking.endTime.toISOString(),
      location: booking.location ?? "",
      rejectionReason: rejectionReason ?? undefined,
    };
    await sendDeclinedEmailsAndSMS(evt, booking.eventType?.metadata as EventTypeMetadata);
  }

  // webhooks
  const teamId = await getTeamIdFromEventType({
    eventType: { team: { id: booking.eventType?.teamId ?? null }, parentId: booking.eventType?.parentId ?? null },
  });
  const orgId = await getOrgIdFromMemberOrTeamId({ memberId: booking.userId, teamId });

  const subscriberOptions: GetSubscriberOptions = {
    userId: booking.userId,
    eventTypeId: booking.eventTypeId,
    triggerEvent: WebhookTriggerEvents.BOOKING_REJECTED,
    teamId,
    orgId,
    oAuthClientId: platformClientParams?.platformClientId,
  };
  const eventTypeInfo: EventTypeInfo = {
    eventTitle: booking.eventType?.title,
    eventDescription: booking.eventType?.description,
    requiresConfirmation: booking.eventType?.requiresConfirmation || null,
    price: booking.eventType?.price,
    currency: booking.eventType?.currency,
    length: booking.eventType?.length,
  };
  const webhookData: EventPayloadType = {
    ...eventTypeInfo,
    bookingId,
    eventTypeId: booking.eventType?.id,
    status: BookingStatus.REJECTED,
    smsReminderNumber: booking.smsReminderNumber || undefined,
  };

  await handleWebhookTrigger({
    subscriberOptions,
    eventTrigger: WebhookTriggerEvents.BOOKING_REJECTED,
    webhookData,
  });
}

// ---------- auth helpers (unchanged) ----------

const checkIfUserIsAuthorizedToConfirmBooking = async ({
  eventTypeId,
  loggedInUserId,
  teamId,
  bookingUserId,
  userRole,
}: {
  eventTypeId: number | null;
  loggedInUserId: number;
  teamId?: number | null;
  bookingUserId: number | null;
  userRole: string;
}): Promise<void> => {
  if (userRole === UserPermissionRole.ADMIN) return;
  if (bookingUserId === loggedInUserId) return;

  if (eventTypeId) {
    const [loggedInUserAsHostOfEventType, loggedInUserAsUserOfEventType] = await Promise.all([
      prisma.eventType.findUnique({
        where: { id: eventTypeId, hosts: { some: { userId: loggedInUserId } } },
        select: { id: true },
      }),
      prisma.eventType.findUnique({
        where: { id: eventTypeId, users: { some: { id: loggedInUserId } } },
        select: { id: true },
      }),
    ]);
    if (loggedInUserAsHostOfEventType || loggedInUserAsUserOfEventType) return;
  }

  if (teamId) {
    const membership = await prisma.membership.findFirst({
      where: {
        userId: loggedInUserId,
        teamId: teamId,
        role: { in: [MembershipRole.OWNER, MembershipRole.ADMIN] },
      },
    });
    if (membership) return;
  }

  if (bookingUserId && (await isLoggedInUserOrgAdminOfBookingUser(loggedInUserId, bookingUserId))) return;

  throw new TRPCError({ code: "UNAUTHORIZED", message: "User is not authorized to confirm this booking" });
};

async function isLoggedInUserOrgAdminOfBookingUser(loggedInUserId: number, bookingUserId: number) {
  const orgIdsWhereLoggedInUserAdmin = await getOrgIdsWhereAdmin(loggedInUserId);
  if (orgIdsWhereLoggedInUserAdmin.length === 0) return false;

  const bookingUserOrgMembership = await prisma.membership.findFirst({
    where: { userId: bookingUserId, teamId: { in: orgIdsWhereLoggedInUserAdmin }, team: { parentId: null } },
  });
  if (bookingUserOrgMembership) return true;

  const bookingUserOrgTeamMembership = await prisma.membership.findFirst({
    where: { userId: bookingUserId, team: { parentId: { in: orgIdsWhereLoggedInUserAdmin } } },
  });
  return !!bookingUserOrgTeamMembership;
}

async function getOrgIdsWhereAdmin(loggedInUserId: number) {
  const loggedInUserOrgMemberships = await prisma.membership.findMany({
    where: { userId: loggedInUserId, role: { in: [MembershipRole.OWNER, MembershipRole.ADMIN] }, team: { parentId: null } },
    select: { teamId: true },
  });
  return loggedInUserOrgMemberships.map((m) => m.teamId);
}
