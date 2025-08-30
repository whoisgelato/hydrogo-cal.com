import type { DestinationCalendar, User } from "@prisma/client";
// eslint-disable-next-line no-restricted-imports
import { cloneDeep } from "lodash";
import short, { uuid } from "short-uuid";
import { v5 as uuidv5 } from "uuid";

import processExternalId from "@calcom/app-store/_utils/calendars/processExternalId";
import { metadata as GoogleMeetMetadata } from "@calcom/app-store/googlevideo/_metadata";
import {
  getLocationValueForDB,
  MeetLocationType,
  OrganizerDefaultConferencingAppType,
} from "@calcom/app-store/locations";
import { getAppFromSlug } from "@calcom/app-store/utils";
import dayjs from "@calcom/dayjs";
import { scheduleMandatoryReminder } from "@calcom/ee/workflows/lib/reminders/scheduleMandatoryReminder";
import {
  sendAttendeeRequestEmailAndSMS,
  sendOrganizerRequestEmail,
  sendRescheduledEmailsAndSMS,
  sendRoundRobinCancelledEmailsAndSMS,
  sendRoundRobinRescheduledEmailsAndSMS,
  sendRoundRobinScheduledEmailsAndSMS,
  sendScheduledEmailsAndSMS,
} from "@calcom/emails";
import getICalUID from "@calcom/emails/lib/getICalUID";
import { CalendarEventBuilder } from "@calcom/features/CalendarEventBuilder";
import { handleWebhookTrigger } from "@calcom/features/bookings/lib/handleWebhookTrigger";
import { isEventTypeLoggingEnabled } from "@calcom/features/bookings/lib/isEventTypeLoggingEnabled";
import AssignmentReasonRecorder from "@calcom/features/ee/round-robin/assignmentReason/AssignmentReasonRecorder";
import {
  allowDisablingAttendeeConfirmationEmails,
  allowDisablingHostConfirmationEmails,
} from "@calcom/features/ee/workflows/lib/allowDisablingStandardEmails";
import { scheduleWorkflowReminders } from "@calcom/features/ee/workflows/lib/reminders/reminderScheduler";
import { getFullName } from "@calcom/features/form-builder/utils";
import { UsersRepository } from "@calcom/features/users/users.repository";
import type { GetSubscriberOptions } from "@calcom/features/webhooks/lib/getWebhooks";
import getWebhooks from "@calcom/features/webhooks/lib/getWebhooks";
import {
  deleteWebhookScheduledTriggers,
  scheduleTrigger,
} from "@calcom/features/webhooks/lib/scheduleTrigger";
import { getVideoCallUrlFromCalEvent } from "@calcom/lib/CalEventParser";
import EventManager, { placeholderCreatedEvent } from "@calcom/lib/EventManager";
import { handleAnalyticsEvents } from "@calcom/lib/analyticsManager/handleAnalyticsEvents";
import { groupHostsByGroupId } from "@calcom/lib/bookings/hostGroupUtils";
import { shouldIgnoreContactOwner } from "@calcom/lib/bookings/routing/utils";
import { DEFAULT_GROUP_ID } from "@calcom/lib/constants";
import { getUsernameList } from "@calcom/lib/defaultEvents";
import {
  enrichHostsWithDelegationCredentials,
  getFirstDelegationConferencingCredentialAppLocation,
} from "@calcom/lib/delegationCredential/server";
import { getCheckBookingAndDurationLimitsService } from "@calcom/lib/di/containers/BookingLimits";
import { getCacheService } from "@calcom/lib/di/containers/Cache";
import { ErrorCode } from "@calcom/lib/errorCodes";
import { getErrorFromUnknown } from "@calcom/lib/errors";
import { getEventName, updateHostInEventName } from "@calcom/lib/event";
import { extractBaseEmail } from "@calcom/lib/extract-base-email";
import { getBookerBaseUrl } from "@calcom/lib/getBookerUrl/server";
import getOrgIdFromMemberOrTeamId from "@calcom/lib/getOrgIdFromMemberOrTeamId";
import { getPaymentAppData } from "@calcom/lib/getPaymentAppData";
import { getTeamIdFromEventType } from "@calcom/lib/getTeamIdFromEventType";
import { HttpError } from "@calcom/lib/http-error";
import logger from "@calcom/lib/logger";
import { handlePayment } from "@calcom/lib/payment/handlePayment";
import { getPiiFreeCalendarEvent, getPiiFreeEventType } from "@calcom/lib/piiFreeData";
import { safeStringify } from "@calcom/lib/safeStringify";
import { getLuckyUser } from "@calcom/lib/server/getLuckyUser";
import { getTranslation } from "@calcom/lib/server/i18n";
import { BookingRepository } from "@calcom/lib/server/repository/booking";
import { WorkflowRepository } from "@calcom/lib/server/repository/workflow";
import { HashedLinkService } from "@calcom/lib/server/service/hashedLinkService";
import { getTimeFormatStringFromUserTimeFormat } from "@calcom/lib/timeFormat";
import prisma from "@calcom/prisma";
import type { AssignmentReasonEnum } from "@calcom/prisma/enums";
import { BookingStatus, SchedulingType, WebhookTriggerEvents } from "@calcom/prisma/enums";
import { CreationSource } from "@calcom/prisma/enums";
import {
  eventTypeAppMetadataOptionalSchema,
  eventTypeMetaDataSchemaWithTypedApps,
} from "@calcom/prisma/zod-utils";
import { userMetadata as userMetadataSchema } from "@calcom/prisma/zod-utils";
import { getAllWorkflowsFromEventType } from "@calcom/trpc/server/routers/viewer/workflows/util";
import type {
  AdditionalInformation,
  AppsStatus,
  CalendarEvent,
  CalEventResponses,
  Person,
} from "@calcom/types/Calendar";
import type { CredentialForCalendarService } from "@calcom/types/Credential";
import type { EventResult, PartialReference } from "@calcom/types/EventManager";

import type { EventPayloadType, EventTypeInfo } from "../../webhooks/lib/sendPayload";
import { getAllCredentialsIncludeServiceAccountKey } from "./getAllCredentialsForUsersOnEvent/getAllCredentials";
import { refreshCredentials } from "./getAllCredentialsForUsersOnEvent/refreshCredentials";
import getBookingDataSchema from "./getBookingDataSchema";
import { addVideoCallDataToEvent } from "./handleNewBooking/addVideoCallDataToEvent";
import { checkActiveBookingsLimitForBooker } from "./handleNewBooking/checkActiveBookingsLimitForBooker";
import { checkIfBookerEmailIsBlocked } from "./handleNewBooking/checkIfBookerEmailIsBlocked";
import { createBooking } from "./handleNewBooking/createBooking";
import type { Booking } from "./handleNewBooking/createBooking";
import { ensureAvailableUsers } from "./handleNewBooking/ensureAvailableUsers";
import { getBookingData } from "./handleNewBooking/getBookingData";
import { getCustomInputsResponses } from "./handleNewBooking/getCustomInputsResponses";
import { getEventType } from "./handleNewBooking/getEventType";
import type { getEventTypeResponse } from "./handleNewBooking/getEventTypesFromDB";
import { getLocationValuesForDb } from "./handleNewBooking/getLocationValuesForDb";
import { getRequiresConfirmationFlags } from "./handleNewBooking/getRequiresConfirmationFlags";
import { getSeatedBooking } from "./handleNewBooking/getSeatedBooking";
import { getVideoCallDetails } from "./handleNewBooking/getVideoCallDetails";
import { handleAppsStatus } from "./handleNewBooking/handleAppsStatus";
import { loadAndValidateUsers } from "./handleNewBooking/loadAndValidateUsers";
import { createLoggerWithEventDetails } from "./handleNewBooking/logger";
import { getOriginalRescheduledBooking } from "./handleNewBooking/originalRescheduledBookingUtils";
import type { BookingType } from "./handleNewBooking/originalRescheduledBookingUtils";
import { scheduleNoShowTriggers } from "./handleNewBooking/scheduleNoShowTriggers";
import type { IEventTypePaymentCredentialType, Invitee, IsFixedAwareUser } from "./handleNewBooking/types";
import { validateBookingTimeIsNotOutOfBounds } from "./handleNewBooking/validateBookingTimeIsNotOutOfBounds";
import { validateEventLength } from "./handleNewBooking/validateEventLength";
import handleSeats from "./handleSeats/handleSeats";

const translator = short();
const log = logger.getSubLogger({ prefix: ["[api] book:user"] });

/** Background helper */
const fireAndForget = (fn: () => Promise<void>) => {
  try {
    if (typeof setImmediate !== "undefined")
      setImmediate(() => void fn().catch((e) => log.error("post-booking", e)));
    else if (typeof queueMicrotask !== "undefined")
      queueMicrotask(() => void fn().catch((e) => log.error("post-booking", e)));
    else void fn().catch((e) => log.error("post-booking", e));
  } catch (e) {
    log.error("post-booking-schedule", e);
  }
};

/** tiny memo-cache for getTranslation to avoid duplicate IO */
const tCache = new Map<string, Awaited<ReturnType<typeof getTranslation>>>();
const getT = async (locale: string | null | undefined) => {
  const key = (locale ?? "en").toLowerCase();
  if (!tCache.has(key)) tCache.set(key, await getTranslation(key, "common"));
  return tCache.get(key)!;
};

/** env read once */
const BLACKLISTED_GUEST_EMAILS =
  process.env.BLACKLISTED_GUEST_EMAILS?.split(",").map((e) => e.trim().toLowerCase()) ?? [];

type IsFixedAwareUserWithCredentials = Omit<IsFixedAwareUser, "credentials"> & {
  credentials: CredentialForCalendarService[];
};

function assertNonEmptyArray<T>(arr: T[]): asserts arr is [T, ...T[]] {
  if (arr.length === 0) {
    throw new Error("Array should have at least one item, but it's empty");
  }
}

function getICalSequence(originalRescheduledBooking: BookingType | null) {
  if (!originalRescheduledBooking) return 0;
  if (!originalRescheduledBooking.iCalSequence) return 1;
  return originalRescheduledBooking.iCalSequence + 1;
}

type BookingDataSchemaGetter =
  | typeof getBookingDataSchema
  | typeof import("@calcom/features/bookings/lib/getBookingDataSchemaForApi").default;

type CreatedBooking = Booking & { appsStatus?: AppsStatus[]; paymentUid?: string; paymentId?: number };
type ReturnTypeCreateBooking = Awaited<ReturnType<typeof createBooking>>;
export const buildDryRunBooking = ({
  eventTypeId,
  organizerUser,
  eventName,
  startTime,
  endTime,
  contactOwnerFromReq,
  contactOwnerEmail,
  allHostUsers,
  isManagedEventType,
}: {
  eventTypeId: number;
  organizerUser: {
    id: number;
    name: string | null;
    username: string | null;
    email: string;
    timeZone: string;
  };
  eventName: string;
  startTime: string;
  endTime: string;
  contactOwnerFromReq: string | null;
  contactOwnerEmail: string | null;
  allHostUsers: { id: number }[];
  isManagedEventType: boolean;
}) => {
  const sanitizedOrganizerUser = {
    id: organizerUser.id,
    name: organizerUser.name,
    username: organizerUser.username,
    email: organizerUser.email,
    timeZone: organizerUser.timeZone,
  };
  const booking = {
    id: -101,
    uid: "DRY_RUN_UID",
    iCalUID: "DRY_RUN_ICAL_UID",
    status: BookingStatus.ACCEPTED,
    eventTypeId: eventTypeId,
    user: sanitizedOrganizerUser,
    userId: sanitizedOrganizerUser.id,
    title: eventName,
    startTime: new Date(startTime),
    endTime: new Date(endTime),
    createdAt: new Date(),
    updatedAt: new Date(),
    attendees: [],
    oneTimePassword: null,
    smsReminderNumber: null,
    metadata: {},
    idempotencyKey: null,
    userPrimaryEmail: null,
    description: null,
    customInputs: null,
    responses: null,
    location: null,
    paid: false,
    cancellationReason: null,
    rejectionReason: null,
    dynamicEventSlugRef: null,
    dynamicGroupSlugRef: null,
    fromReschedule: null,
    recurringEventId: null,
    scheduledJobs: [],
    rescheduledBy: null,
    destinationCalendarId: null,
    reassignReason: null,
    reassignById: null,
    rescheduled: false,
    isRecorded: false,
    iCalSequence: 0,
    rating: null,
    ratingFeedback: null,
    noShowHost: null,
    cancelledBy: null,
    creationSource: CreationSource.WEBAPP,
    references: [],
    payment: [],
  } satisfies ReturnTypeCreateBooking;

  const troubleshooterData = {
    organizerUserId: organizerUser.id,
    eventTypeId,
    askedContactOwnerEmail: contactOwnerFromReq,
    usedContactOwnerEmail: contactOwnerEmail,
    allHostUsers: allHostUsers.map((user) => user.id),
    isManagedEventType: isManagedEventType,
  };

  return { booking, troubleshooterData };
};

const buildDryRunEventManager = () => ({
  create: async () => ({ results: [], referencesToCreate: [] }),
  reschedule: async () => ({ results: [], referencesToCreate: [] }),
});

export const buildEventForTeamEventType = async ({
  existingEvent: evt,
  users,
  organizerUser,
  schedulingType,
  team,
}: {
  existingEvent: Partial<CalendarEvent>;
  users: (Pick<User, "id" | "name" | "timeZone" | "locale" | "email"> & {
    destinationCalendar: DestinationCalendar | null;
    isFixed?: boolean;
  })[];
  organizerUser: { email: string };
  schedulingType: SchedulingType | null;
  team?: { id: number; name: string } | null;
}) => {
  if (!schedulingType) {
    throw new Error("Scheduling type is required for team event type");
  }
  const teamDestinationCalendars: DestinationCalendar[] = [];
  const fixedUsers = users.filter((user) => user.isFixed);
  const nonFixedUsers = users.filter((user) => !user.isFixed);
  const filteredUsers =
    schedulingType === SchedulingType.ROUND_ROBIN ? [...fixedUsers, ...nonFixedUsers] : users;

  const teamMemberPromises = filteredUsers
    .filter((user) => user.email !== organizerUser.email)
    .map(async (user) => {
      if (schedulingType === "COLLECTIVE" && user.destinationCalendar) {
        teamDestinationCalendars.push({
          ...user.destinationCalendar,
          externalId: processExternalId(user.destinationCalendar),
        });
      }

      return {
        id: user.id,
        email: user.email ?? "",
        name: user.name ?? "",
        firstName: "",
        lastName: "",
        timeZone: user.timeZone,
        language: {
          translate: await getT(user.locale),
          locale: user.locale ?? "en",
        },
      };
    });

  const teamMembers = await Promise.all(teamMemberPromises);

  const updatedEvt = CalendarEventBuilder.fromEvent(evt)
    ?.withDestinationCalendar([...(evt.destinationCalendar ?? []), ...teamDestinationCalendars])
    .build();

  if (!updatedEvt) {
    throw new HttpError({
      statusCode: 400,
      message: "Failed to build event with destination calendar due to missing required fields",
    });
  }

  const teamEvt = CalendarEventBuilder.fromEvent(updatedEvt)
    ?.withTeam({ members: teamMembers, name: team?.name || "Nameless", id: team?.id ?? 0 })
    .build();

  if (!teamEvt) {
    throw new HttpError({
      statusCode: 400,
      message: "Failed to build team event due to missing required fields",
    });
  }

  return teamEvt;
};

function buildTroubleshooterData({
  eventType,
}: {
  eventType: { id: number; slug: string };
}) {
  return {
    organizerUser: null as { id: number } | null,
    eventType: { id: eventType.id, slug: eventType.slug },
    allHostUsers: [] as number[],
    luckyUsers: [] as number[],
    luckyUserPool: [] as number[],
    fixedUsers: [] as number[],
    luckyUsersFromFirstBooking: [] as number[],
    usedContactOwnerEmail: null as string | null,
    askedContactOwnerEmail: null as string | null,
    isManagedEventType: false,
  };
}

export type PlatformParams = {
  platformClientId?: string;
  platformCancelUrl?: string;
  platformBookingUrl?: string;
  platformRescheduleUrl?: string;
  platformBookingLocation?: string;
  areCalendarEventsEnabled?: boolean;
};

export type BookingHandlerInput = {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  bookingData: Record<string, any>;
  userId?: number;
  hostname?: string;
  forcedSlug?: string;
} & PlatformParams;

function formatAvailabilitySnapshot(data: {
  dateRanges: { start: dayjs.Dayjs; end: dayjs.Dayjs }[];
  oooExcludedDateRanges: { start: dayjs.Dayjs; end: dayjs.Dayjs }[];
}) {
  return {
    ...data,
    dateRanges: data.dateRanges.map(({ start, end }) => ({ start: start.toISOString(), end: end.toISOString() })),
    oooExcludedDateRanges: data.oooExcludedDateRanges.map(({ start, end }) => ({
      start: start.toISOString(),
      end: end.toISOString(),
    })),
  };
}

async function handler(
  input: BookingHandlerInput,
  bookingDataSchemaGetter: BookingDataSchemaGetter = getBookingDataSchema
) {
  const {
    bookingData: rawBookingData,
    userId,
    platformClientId,
    platformCancelUrl,
    platformBookingUrl,
    platformRescheduleUrl,
    platformBookingLocation,
    hostname,
    forcedSlug,
    areCalendarEventsEnabled = true,
  } = input;

  const isPlatformBooking = !!platformClientId;

  const eventType = await getEventType({
    eventTypeId: rawBookingData.eventTypeId,
    eventTypeSlug: rawBookingData.eventTypeSlug,
  });

  const bookingDataSchema = bookingDataSchemaGetter({
    view: rawBookingData.rescheduleUid ? "reschedule" : "booking",
    bookingFields: eventType.bookingFields,
  });

  const bookingData = await getBookingData({
    reqBody: rawBookingData,
    eventType,
    schema: bookingDataSchema,
  });

  const {
    recurringCount,
    noEmail,
    eventTypeId,
    eventTypeSlug,
    hasHashedBookingLink,
    language,
    appsStatus: reqAppsStatus,
    name: bookerName,
    attendeePhoneNumber: bookerPhoneNumber,
    email: bookerEmail,
    guests: reqGuests,
    location,
    notes: additionalNotes,
    smsReminderNumber,
    rescheduleReason,
    luckyUsers,
    routedTeamMemberIds,
    reroutingFormResponses,
    routingFormResponseId,
    _isDryRun: isDryRun = false,
    _shouldServeCache,
    ...reqBody
  } = bookingData;

  let troubleshooterData = buildTroubleshooterData({ eventType });
  const loggerWithEventDetails = createLoggerWithEventDetails(eventTypeId, reqBody.user, eventTypeSlug);

  // run simple checks in parallel when possible
  await Promise.all([
    checkIfBookerEmailIsBlocked({ loggedInUserId: userId, bookerEmail }),
    (async () => {
      if (!rawBookingData.rescheduleUid) {
        await checkActiveBookingsLimitForBooker({
          eventTypeId,
          maxActiveBookingsPerBooker: eventType.maxActiveBookingsPerBooker,
          bookerEmail,
          offerToRescheduleLastBooking: eventType.maxActiveBookingPerBookerOfferReschedule,
        });
      }
    })(),
  ]);

  if (isEventTypeLoggingEnabled({ eventTypeId, usernameOrTeamName: reqBody.user })) {
    logger.settings.minLevel = 0;
  }

  const fullName = getFullName(bookerName);
  // only load guest translator if we actually have guests later
  const dynamicUserList = Array.isArray(reqBody.user) ? reqBody.user : getUsernameList(reqBody.user);
  if (!eventType) throw new HttpError({ statusCode: 404, message: "event_type_not_found" });

  if (eventType.seatsPerTimeSlot && eventType.recurringEvent) {
    throw new HttpError({ statusCode: 400, message: "recurring_event_seats_error" });
  }

  const bookingSeat = reqBody.rescheduleUid ? await getSeatedBooking(reqBody.rescheduleUid) : null;
  const rescheduleUid = bookingSeat ? bookingSeat.booking.uid : reqBody.rescheduleUid;

  let originalRescheduledBooking = rescheduleUid
    ? await getOriginalRescheduledBooking(rescheduleUid, !!eventType.seatsPerTimeSlot)
    : null;

  const paymentAppData = getPaymentAppData({
    ...eventType,
    metadata: eventTypeMetaDataSchemaWithTypedApps.parse(eventType.metadata),
  });

  const startUtc = dayjs(reqBody.start).utc().format();
  const endUtc = dayjs(reqBody.end).utc().format();

  const [{ userReschedulingIsOwner, isConfirmedByDefault }] = await Promise.all([
    getRequiresConfirmationFlags({
      eventType,
      bookingStartTime: reqBody.start,
      userId,
      originalRescheduledBookingOrganizerId: originalRescheduledBooking?.user?.id,
      paymentAppData,
      bookerEmail,
    }),
  ]);

  // For unconfirmed bookings or RR with same attendee & timeslot: return original booking if exists
  if ((!isConfirmedByDefault && !userReschedulingIsOwner) || eventType.schedulingType === SchedulingType.ROUND_ROBIN) {
    const bookingRepo = new BookingRepository(prisma);
    const requiresPayment = !Number.isNaN(paymentAppData.price) && paymentAppData.price > 0;

    const existingBooking = await bookingRepo.getValidBookingFromEventTypeForAttendee({
      eventTypeId,
      bookerEmail,
      bookerPhoneNumber,
      startTime: new Date(startUtc),
      filterForUnconfirmed: !isConfirmedByDefault,
    });

    if (existingBooking) {
      const hasPayments = existingBooking.payment.length > 0;
      const isPaidBooking = existingBooking.paid || !hasPayments;
      const shouldShowPaymentForm = requiresPayment && !isPaidBooking;
      const firstPayment = shouldShowPaymentForm ? existingBooking.payment[0] : undefined;

      const bookingResponse = {
        ...existingBooking,
        user: { ...existingBooking.user, email: null },
        paymentRequired: shouldShowPaymentForm,
        seatReferenceUid: "",
      };

      return {
        ...bookingResponse,
        luckyUsers: bookingResponse.userId ? [bookingResponse.userId] : [],
        isDryRun,
        ...(isDryRun ? { troubleshooterData } : {}),
        paymentUid: firstPayment?.uid,
        paymentId: firstPayment?.id,
      };
    }
  }

  const cacheService = getCacheService();
  const shouldServeCache = await cacheService.getShouldServeCache(_shouldServeCache, eventType.team?.id);

  const isTeamEventType =
    !!eventType.schedulingType && ["COLLECTIVE", "ROUND_ROBIN"].includes(eventType.schedulingType);

  loggerWithEventDetails.info(
    `Booking eventType ${eventTypeId} started`,
    safeStringify({
      reqBody: {
        user: reqBody.user,
        eventTypeId,
        eventTypeSlug,
        startTime: reqBody.start,
        endTime: reqBody.end,
        rescheduleUid: reqBody.rescheduleUid,
        location: location,
        timeZone: reqBody.timeZone,
      },
      isTeamEventType,
      eventType: getPiiFreeEventType(eventType),
      dynamicUserList,
      paymentAppData: {
        enabled: paymentAppData.enabled,
        price: paymentAppData.price,
        paymentOption: paymentAppData.paymentOption,
        currency: paymentAppData.currency,
        appId: paymentAppData.appId,
      },
    })
  );

  const user = eventType.users.find((u) => u.id === eventType.userId);
  const userSchedule = user?.schedules.find((s) => s.id === user?.defaultScheduleId);
  const eventTimeZone = eventType.schedule?.timeZone ?? userSchedule?.timeZone;

  await validateBookingTimeIsNotOutOfBounds<typeof eventType>(
    reqBody.start,
    reqBody.timeZone,
    eventType,
    eventTimeZone,
    loggerWithEventDetails
  );

  validateEventLength({
    reqBodyStart: reqBody.start,
    reqBodyEnd: reqBody.end,
    eventTypeMultipleDuration: eventType.metadata?.multipleDuration,
    eventTypeLength: eventType.length,
    logger: loggerWithEventDetails,
  });

  const contactOwnerFromReq = reqBody.teamMemberEmail ?? null;

  const skipContactOwner = shouldIgnoreContactOwner({
    skipContactOwner: reqBody.skipContactOwner ?? null,
    rescheduleUid: reqBody.rescheduleUid ?? null,
    routedTeamMemberIds: routedTeamMemberIds ?? null,
  });

  const contactOwnerEmail = skipContactOwner ? null : contactOwnerFromReq;
  const crmRecordId: string | undefined = reqBody.crmRecordId ?? undefined;

  let routingFormResponse = null;

  if (routedTeamMemberIds) {
    if (routingFormResponseId === undefined) {
      throw new HttpError({ statusCode: 400, message: "Missing routingFormResponseId" });
    }
    routingFormResponse = await prisma.app_RoutingForms_FormResponse.findUnique({
      where: { id: routingFormResponseId },
      select: { response: true, form: { select: { routes: true, fields: true } }, chosenRouteId: true },
    });
  }

  const { qualifiedRRUsers, additionalFallbackRRUsers, fixedUsers } = await loadAndValidateUsers({
    hostname,
    forcedSlug,
    isPlatform: isPlatformBooking,
    eventType,
    eventTypeId,
    dynamicUserList,
    logger: loggerWithEventDetails,
    routedTeamMemberIds: routedTeamMemberIds ?? null,
    contactOwnerEmail,
    rescheduleUid: reqBody.rescheduleUid || null,
    routingFormResponse,
  });

  let users = [...qualifiedRRUsers, ...additionalFallbackRRUsers, ...fixedUsers];
  const firstUser = users[0];

  let { locationBodyString, organizerOrFirstDynamicGroupMemberDefaultLocationUrl } = getLocationValuesForDb({
    dynamicUserList,
    users,
    location,
  });

  const checkBookingAndDurationLimitsService = getCheckBookingAndDurationLimitsService();
  await checkBookingAndDurationLimitsService.checkBookingAndDurationLimits({
    eventType,
    reqBodyStart: reqBody.start,
    reqBodyRescheduleUid: reqBody.rescheduleUid,
  });

  let luckyUserResponse: { luckyUsers: number[] } | undefined;
  let isFirstSeat = true;
  let availableUsers: IsFixedAwareUser[] = [];

  if (eventType.seatsPerTimeSlot) {
    const booking = await prisma.booking.findFirst({
      where: {
        eventTypeId: eventType.id,
        startTime: new Date(startUtc),
        status: BookingStatus.ACCEPTED,
      },
      select: { userId: true, attendees: { select: { email: true } } },
    });

    if (booking) {
      isFirstSeat = false;
      if (eventType.schedulingType === SchedulingType.ROUND_ROBIN) {
        const fixedHosts = users.filter((u) => u.isFixed);
        const originalNonFixedHost = users.find((u) => !u.isFixed && u.id === booking.userId);

        if (originalNonFixedHost) {
          users = [...fixedHosts, originalNonFixedHost];
        } else {
          const attendeeEmailSet = new Set(booking.attendees.map((a) => a.email));
          const nonFixedAttendeeHost = users.find((u) => !u.isFixed && attendeeEmailSet.has(u.email));
          users = [...fixedHosts, ...(nonFixedAttendeeHost ? [nonFixedAttendeeHost] : [])];
        }
      }
    }
  }

  if (isFirstSeat) {
    const eventTypeWithUsers: Omit<getEventTypeResponse, "users"> & { users: IsFixedAwareUserWithCredentials[] } = {
      ...eventType,
      users: users as IsFixedAwareUserWithCredentials[],
      ...(eventType.recurringEvent && {
        recurringEvent: { ...eventType.recurringEvent, count: recurringCount || eventType.recurringEvent.count },
      }),
    };

    if (input.bookingData.allRecurringDates && input.bookingData.isFirstRecurringSlot) {
      const isTeamEvent =
        eventType.schedulingType === SchedulingType.COLLECTIVE ||
        eventType.schedulingType === SchedulingType.ROUND_ROBIN;

      const _fixedUsers = isTeamEvent
        ? eventTypeWithUsers.users.filter((u: IsFixedAwareUserWithCredentials) => u.isFixed)
        : [];

      const limit = Math.min(
        input.bookingData.allRecurringDates.length,
        input.bookingData.numSlotsToCheckForAvailability
      );

      if (isTeamEvent) {
        // check each fixed user, but cap checks to `limit`
        for (let i = 0; i < limit; i++) {
          const { start, end } = input.bookingData.allRecurringDates[i];
          for (const fu of _fixedUsers) {
            await ensureAvailableUsers(
              { ...eventTypeWithUsers, users: [fu] },
              {
                dateFrom: dayjs(start).tz(reqBody.timeZone).format(),
                dateTo: dayjs(end).tz(reqBody.timeZone).format(),
                timeZone: reqBody.timeZone,
                originalRescheduledBooking: originalRescheduledBooking ?? null,
              },
              loggerWithEventDetails,
              shouldServeCache
            );
          }
        }
      } else {
        for (let i = 0; i < limit; i++) {
          const { start, end } = input.bookingData.allRecurringDates[i];
          await ensureAvailableUsers(
            eventTypeWithUsers,
            {
              dateFrom: dayjs(start).tz(reqBody.timeZone).format(),
              dateTo: dayjs(end).tz(reqBody.timeZone).format(),
              timeZone: reqBody.timeZone,
              originalRescheduledBooking,
            },
            loggerWithEventDetails,
            shouldServeCache
          );
        }
      }
    }

    if (!input.bookingData.allRecurringDates || input.bookingData.isFirstRecurringSlot) {
      try {
        availableUsers = await ensureAvailableUsers(
          { ...eventTypeWithUsers, users: [...qualifiedRRUsers, ...fixedUsers] as IsFixedAwareUser[] },
          {
            dateFrom: dayjs(reqBody.start).tz(reqBody.timeZone).format(),
            dateTo: dayjs(reqBody.end).tz(reqBody.timeZone).format(),
            timeZone: reqBody.timeZone,
            originalRescheduledBooking,
          },
          loggerWithEventDetails,
          shouldServeCache
        );
      } catch {
        if (additionalFallbackRRUsers.length) {
          loggerWithEventDetails.debug(
            "Qualified users not available, check for fallback users",
            safeStringify({
              qualifiedRRUsers: qualifiedRRUsers.map((u) => u.id),
              additionalFallbackRRUsers: additionalFallbackRRUsers.map((u) => u.id),
            })
          );
          availableUsers = await ensureAvailableUsers(
            { ...eventTypeWithUsers, users: [...additionalFallbackRRUsers, ...fixedUsers] as IsFixedAwareUser[] },
            {
              dateFrom: dayjs(reqBody.start).tz(reqBody.timeZone).format(),
              dateTo: dayjs(reqBody.end).tz(reqBody.timeZone).format(),
              timeZone: reqBody.timeZone,
              originalRescheduledBooking,
            },
            loggerWithEventDetails,
            shouldServeCache
          );
        } else {
          loggerWithEventDetails.debug(
            "Qualified users not available, no fallback users",
            safeStringify({ qualifiedRRUsers: qualifiedRRUsers.map((u) => u.id) })
          );
          throw new Error(ErrorCode.NoAvailableUsersFound);
        }
      }

      const fixedUserPool: IsFixedAwareUser[] = [];
      const nonFixedUsers: IsFixedAwareUser[] = [];

      for (const u of availableUsers) (u.isFixed ? fixedUserPool : nonFixedUsers).push(u);

      const luckyUserPools = groupHostsByGroupId({ hosts: nonFixedUsers, hostGroups: eventType.hostGroups });
      const notAvailableLuckyUsers: typeof users = [];

      loggerWithEventDetails.debug(
        "Computed available users",
        safeStringify({
          availableUsers: availableUsers.map((u) => u.id),
          luckyUserPools: Object.fromEntries(
            Object.entries(luckyUserPools).map(([gid, us]) => [gid, us.map((u) => u.id)])
          ),
        })
      );

      const _luckyUsers: typeof users = [];
      for (const [groupId, luckyUserPool] of Object.entries(luckyUserPools)) {
        let luckUserFound = false;
        while (luckyUserPool.length > 0 && !luckUserFound) {
          const freeUsers = luckyUserPool.filter(
            (u) => !_luckyUsers.concat(notAvailableLuckyUsers).some((e) => e.id === u.id)
          );
          if (freeUsers.length === 0) break;
          assertNonEmptyArray(freeUsers);

          const userIdsSet = new Set(users.map((u) => u.id));
          const firstUserOrgId = await getOrgIdFromMemberOrTeamId({
            memberId: eventTypeWithUsers.users[0].id ?? null,
            teamId: eventType.teamId,
          });

          const newLuckyUser = await getLuckyUser({
            availableUsers: freeUsers,
            allRRHosts: (
              await enrichHostsWithDelegationCredentials({ orgId: firstUserOrgId ?? null, hosts: eventTypeWithUsers.hosts })
            ).filter(
              (host) =>
                !host.isFixed &&
                userIdsSet.has(host.user.id) &&
                (host.groupId === groupId || (!host.groupId && groupId === DEFAULT_GROUP_ID))
            ),
            eventType,
            routingFormResponse,
            meetingStartTime: new Date(reqBody.start),
          });

          if (!newLuckyUser) break;

          if (input.bookingData.isFirstRecurringSlot && eventType.schedulingType === SchedulingType.ROUND_ROBIN) {
            try {
              const limit = Math.min(
                input.bookingData.allRecurringDates.length,
                input.bookingData.numSlotsToCheckForAvailability
              );
              for (let i = 0; i < limit; i++) {
                const { start, end } = input.bookingData.allRecurringDates[i];
                await ensureAvailableUsers(
                  { ...eventTypeWithUsers, users: [newLuckyUser] },
                  {
                    dateFrom: dayjs(start).tz(reqBody.timeZone).format(),
                    dateTo: dayjs(end).tz(reqBody.timeZone).format(),
                    timeZone: reqBody.timeZone,
                    originalRescheduledBooking,
                  },
                  loggerWithEventDetails,
                  shouldServeCache
                );
              }
              _luckyUsers.push(newLuckyUser);
              luckUserFound = true;
            } catch {
              notAvailableLuckyUsers.push(newLuckyUser);
              loggerWithEventDetails.info(
                `Round robin host ${newLuckyUser.name} not available for first two slots. Trying to find another host.`
              );
            }
          } else {
            _luckyUsers.push(newLuckyUser);
            luckUserFound = true;
          }
        }
      }

      if (fixedUserPool.length !== users.filter((u) => u.isFixed).length) {
        throw new Error(ErrorCode.FixedHostsUnavailableForBooking);
      }

      const roundRobinHosts = eventType.hosts.filter((h) => !h.isFixed);
      const hostGroups = groupHostsByGroupId({ hosts: roundRobinHosts, hostGroups: eventType.hostGroups });
      const nonEmptyHostGroups = Object.fromEntries(Object.entries(hostGroups).filter(([, hosts]) => hosts.length > 0));

      if ([...qualifiedRRUsers, ...additionalFallbackRRUsers].length > 0 && _luckyUsers.length !== (Object.keys(nonEmptyHostGroups).length || 1)) {
        throw new Error(ErrorCode.RoundRobinHostsUnavailableForBooking);
      }

      users = [...fixedUserPool, ..._luckyUsers];
      luckyUserResponse = { luckyUsers: _luckyUsers.map((u) => u.id) };
      troubleshooterData = {
        ...troubleshooterData,
        luckyUsers: _luckyUsers.map((u) => u.id),
        fixedUsers: fixedUserPool.map((u) => u.id),
        luckyUserPool: Object.values(luckyUserPools).flat().map((u) => u.id),
      };
    } else if (input.bookingData.allRecurringDates && eventType.schedulingType === SchedulingType.ROUND_ROBIN) {
      const luckyUsersFromFirstBooking = luckyUsers
        ? eventTypeWithUsers.users.filter((u) => luckyUsers.includes(u.id))
        : [];
      const fixedHosts = eventTypeWithUsers.users.filter((u: IsFixedAwareUser) => u.isFixed);
      users = [...fixedHosts, ...luckyUsersFromFirstBooking];
      troubleshooterData = {
        ...troubleshooterData,
        luckyUsersFromFirstBooking: luckyUsersFromFirstBooking.map((u) => u.id),
        fixedUsers: fixedHosts.map((u) => u.id),
      };
    }
  }

  if (users.length === 0 && eventType.schedulingType === SchedulingType.ROUND_ROBIN) {
    loggerWithEventDetails.error("No available users found for round robin event.");
    throw new Error(ErrorCode.RoundRobinHostsUnavailableForBooking);
  }

  const organizerUser = reqBody.teamMemberEmail
    ? users.find((u) => u.email === reqBody.teamMemberEmail) ?? users[0]
    : users[0];

  const tOrganizer = await getT(organizerUser?.locale);
  const allCredentials = await getAllCredentialsIncludeServiceAccountKey(organizerUser, eventType);

  const attendeeInfoOnReschedule =
    userReschedulingIsOwner && originalRescheduledBooking
      ? originalRescheduledBooking.attendees.find((a) => a.email === bookerEmail)
      : null;

  const attendeeLanguage = attendeeInfoOnReschedule ? attendeeInfoOnReschedule.locale : language;
  const attendeeTimezone = attendeeInfoOnReschedule ? attendeeInfoOnReschedule.timeZone : reqBody.timeZone;

  const tAttendees = await getT(attendeeLanguage);

  const isManagedEventType = !!eventType.parentId;

  if (locationBodyString.trim().length === 0) {
    locationBodyString = eventType.locations[0]?.type ?? OrganizerDefaultConferencingAppType;
  }

  const organizationDefaultLocation = getFirstDelegationConferencingCredentialAppLocation({
    credentials: firstUser.credentials,
  });

  if (locationBodyString === OrganizerDefaultConferencingAppType) {
    const metadataParseResult = userMetadataSchema.safeParse(organizerUser.metadata);
    const organizerMetadata = metadataParseResult.success ? metadataParseResult.data : undefined;
    if (organizerMetadata?.defaultConferencingApp?.appSlug) {
      const app = getAppFromSlug(organizerMetadata?.defaultConferencingApp?.appSlug);
      locationBodyString = app?.appData?.location?.type || locationBodyString;
      if (isManagedEventType || isTeamEventType) {
        organizerOrFirstDynamicGroupMemberDefaultLocationUrl = organizerMetadata?.defaultConferencingApp?.appLink;
      }
    } else if (organizationDefaultLocation) {
      locationBodyString = organizationDefaultLocation;
    } else {
      locationBodyString = "integrations:daily";
    }
  }

  const invitee: Invitee = [
    {
      email: bookerEmail,
      name: fullName,
      phoneNumber: bookerPhoneNumber,
      firstName: (typeof bookerName === "object" && bookerName.firstName) || "",
      lastName: (typeof bookerName === "object" && bookerName.lastName) || "",
      timeZone: attendeeTimezone,
      language: { translate: tAttendees, locale: attendeeLanguage ?? "en" },
    },
  ];

  const guests: Invitee = [];
  const guestsRemoved: string[] = [];
  if (Array.isArray(reqGuests) && reqGuests.length) {
    const tGuests = await getT("en");
    for (const g of reqGuests) {
      const baseGuestEmail = extractBaseEmail(g).toLowerCase();
      if (BLACKLISTED_GUEST_EMAILS.includes(baseGuestEmail)) {
        guestsRemoved.push(g);
        continue;
      }
      if (isTeamEventType && users.some((u) => u.email === g)) continue;
      guests.push({
        email: g,
        name: "",
        firstName: "",
        lastName: "",
        timeZone: attendeeTimezone,
        language: { translate: tGuests, locale: "en" },
      });
    }
    if (guestsRemoved.length > 0) log.info("Removed guests from the booking", guestsRemoved);
  }

  const seed = `${organizerUser.username}:${startUtc}:${Date.now()}`;
  const uid = translator.fromUUID(uuidv5(seed, uuidv5.URL));

  const { bookingLocation, conferenceCredentialId } = organizerOrFirstDynamicGroupMemberDefaultLocationUrl
    ? { bookingLocation: organizerOrFirstDynamicGroupMemberDefaultLocationUrl, conferenceCredentialId: undefined }
    : getLocationValueForDB(locationBodyString, eventType.locations);

  const customInputs = getCustomInputsResponses(reqBody, eventType.customInputs);
  const attendeesList = [...invitee, ...guests];

  const responses = reqBody.responses || null;
  const evtName = !eventType?.isDynamic ? eventType.eventName : responses?.title;
  const eventNameObject = {
    attendeeName: fullName || "Nameless",
    eventType: eventType.title,
    eventName: evtName,
    teamName: eventType.schedulingType === "COLLECTIVE" || users.length > 1 ? eventType.team?.name : null,
    host: organizerUser.name || "Nameless",
    location: bookingLocation,
    eventDuration: dayjs(reqBody.end).diff(reqBody.start, "minutes"),
    bookingFields: { ...responses },
    t: tOrganizer,
  };

  const iCalUID = getICalUID({
    event: { iCalUID: originalRescheduledBooking?.iCalUID, uid: originalRescheduledBooking?.uid },
    uid,
  });
  const iCalSequence = getICalSequence(originalRescheduledBooking);

  const organizerOrganizationProfile = await prisma.profile.findFirst({
    where: { userId: organizerUser.id, username: dynamicUserList[0] },
  });

  const organizerOrganizationId = organizerOrganizationProfile?.organizationId;
  const bookerUrl = eventType.team
    ? await getBookerBaseUrl(eventType.team.parentId)
    : await getBookerBaseUrl(organizerOrganizationId ?? null);

  const destinationCalendar = eventType.destinationCalendar
    ? [eventType.destinationCalendar]
    : organizerUser.destinationCalendar
    ? [organizerUser.destinationCalendar]
    : null;

  let organizerEmail = organizerUser.email || "Email-less";
  if (eventType.useEventTypeDestinationCalendarEmail && destinationCalendar?.[0]?.primaryEmail) {
    organizerEmail = destinationCalendar[0].primaryEmail;
  } else if (eventType.secondaryEmailId && eventType.secondaryEmail?.email) {
    organizerEmail = eventType.secondaryEmail.email;
  }

  if (reqBody.calEventResponses) {
    reqBody.calEventResponses["location"].value = {
      value: platformBookingLocation ?? bookingLocation,
      optionValue: "",
    };
  }

  const eventName = getEventName(eventNameObject);

  const builtEvt = new CalendarEventBuilder()
    .withBasicDetails({ bookerUrl, title: eventName, startTime: startUtc, endTime: endUtc, additionalNotes })
    .withEventType({
      slug: eventType.slug,
      description: eventType.description,
      id: eventType.id,
      hideCalendarNotes: eventType.hideCalendarNotes,
      hideCalendarEventDetails: eventType.hideCalendarEventDetails,
      hideOrganizerEmail: eventType.hideOrganizerEmail,
      schedulingType: eventType.schedulingType,
      seatsPerTimeSlot: eventType.seatsPerTimeSlot,
      seatsShowAttendees: eventType.seatsPerTimeSlot ? eventType.seatsShowAttendees : true,
      seatsShowAvailabilityCount: eventType.seatsPerTimeSlot ? eventType.seatsShowAvailabilityCount : true,
      customReplyToEmail: eventType.customReplyToEmail,
      disableRescheduling: eventType.disableRescheduling ?? false,
      disableCancelling: eventType.disableCancelling ?? false,
    })
    .withOrganizer({
      id: organizerUser.id,
      name: organizerUser.name || "Nameless",
      email: organizerEmail,
      username: organizerUser.username || undefined,
      timeZone: organizerUser.timeZone,
      language: { translate: tOrganizer, locale: organizerUser.locale ?? "en" },
      timeFormat: getTimeFormatStringFromUserTimeFormat(organizerUser.timeFormat),
    })
    .withAttendees(attendeesList)
    .withMetadataAndResponses({
      additionalNotes,
      customInputs,
      responses: reqBody.calEventResponses || null,
      userFieldsResponses: reqBody.calEventUserFieldsResponses || null,
    })
    .withLocation({ location: platformBookingLocation ?? bookingLocation, conferenceCredentialId })
    .withDestinationCalendar(destinationCalendar)
    .withIdentifiers({ iCalUID, iCalSequence })
    .withConfirmation({ requiresConfirmation: !isConfirmedByDefault, isConfirmedByDefault })
    .withPlatformVariables({ platformClientId, platformRescheduleUrl, platformCancelUrl, platformBookingUrl })
    .build();

  if (!builtEvt) {
    throw new HttpError({ statusCode: 400, message: "Failed to build calendar event due to missing required fields" });
  }

  let evt: CalendarEvent = builtEvt;

  if (input.bookingData.thirdPartyRecurringEventId) {
    const updatedEvt = CalendarEventBuilder.fromEvent(evt)
      ?.withRecurringEventId(input.bookingData.thirdPartyRecurringEventId)
      .build();
    if (!updatedEvt) {
      throw new HttpError({
        statusCode: 400,
        message: "Failed to build event with recurring event ID due to missing required fields",
      });
    }
    evt = updatedEvt;
  }

  if (isTeamEventType) {
    const teamEvt = await buildEventForTeamEventType({
      existingEvent: evt,
      schedulingType: eventType.schedulingType,
      users,
      team: eventType.team,
      organizerUser,
    });
    if (!teamEvt) throw new HttpError({ statusCode: 400, message: "Failed to build team event" });
    evt = teamEvt;
  }

  const eventTypeInfo: EventTypeInfo = {
    eventTitle: eventType.title,
    eventDescription: eventType.description,
    price: paymentAppData.price,
    currency: eventType.currency,
    length: dayjs(reqBody.end).diff(dayjs(reqBody.start), "minutes"),
  };

  const teamId = await getTeamIdFromEventType({ eventType });
  const triggerForUser = !teamId || (teamId && eventType.parentId);
  const organizerUserId = triggerForUser ? organizerUser.id : null;
  const orgId = await getOrgIdFromMemberOrTeamId({ memberId: organizerUserId, teamId });

  const subscriberOptions: GetSubscriberOptions = {
    userId: organizerUserId,
    eventTypeId,
    triggerEvent: rescheduleUid ? WebhookTriggerEvents.BOOKING_RESCHEDULED : WebhookTriggerEvents.BOOKING_CREATED,
    teamId,
    orgId,
    oAuthClientId: platformClientId,
  };

  const eventTrigger: WebhookTriggerEvents = rescheduleUid
    ? WebhookTriggerEvents.BOOKING_RESCHEDULED
    : WebhookTriggerEvents.BOOKING_CREATED;

  const subscriberOptionsMeetingEnded = {
    userId: triggerForUser ? organizerUser.id : null,
    eventTypeId,
    triggerEvent: WebhookTriggerEvents.MEETING_ENDED,
    teamId,
    orgId,
    oAuthClientId: platformClientId,
  };

  const subscriberOptionsMeetingStarted = {
    userId: triggerForUser ? organizerUser.id : null,
    eventTypeId,
    triggerEvent: WebhookTriggerEvents.MEETING_STARTED,
    teamId,
    orgId,
    oAuthClientId: platformClientId,
  };

  // Lazy-load workflows only when needed (email/reminders paths)
  let workflowsPromise: Promise<ReturnType<typeof getAllWorkflowsFromEventType>> | null = null;
  const loadWorkflows = () => {
    if (!workflowsPromise) {
      workflowsPromise = getAllWorkflowsFromEventType(
        { ...eventType, metadata: eventTypeMetaDataSchemaWithTypedApps.parse(eventType.metadata) },
        organizerUser.id
      );
    }
    return workflowsPromise;
  };

  // Seats flow can diverge to existing booking
  if (eventType.seatsPerTimeSlot) {
    const newBooking = await handleSeats({
      rescheduleUid,
      reqBookingUid: reqBody.bookingUid,
      eventType,
      evt: { ...evt, bookerUrl },
      invitee,
      allCredentials,
      organizerUser,
      originalRescheduledBooking,
      bookerEmail,
      bookerPhoneNumber,
      tAttendees,
      bookingSeat,
      reqUserId: input.userId,
      rescheduleReason,
      reqBodyUser: reqBody.user,
      noEmail,
      isConfirmedByDefault,
      additionalNotes,
      reqAppsStatus,
      attendeeLanguage,
      paymentAppData,
      fullName,
      smsReminderNumber,
      eventTypeInfo,
      uid,
      eventTypeId,
      reqBodyMetadata: reqBody.metadata,
      subscriberOptions,
      eventTrigger,
      responses,
      workflows: await loadWorkflows(),
      rescheduledBy: reqBody.rescheduledBy,
      isDryRun,
    });

    if (newBooking) {
      const bookingResponse = {
        ...newBooking,
        user: { ...newBooking.user, email: null },
        paymentRequired: false,
        isDryRun,
        ...(isDryRun ? { troubleshooterData } : {}),
      };
      return { ...bookingResponse, ...luckyUserResponse };
    } else {
      originalRescheduledBooking = null;
      const updatedEvt = CalendarEventBuilder.fromEvent(evt)
        ?.withIdentifiers({ iCalUID: getICalUID({ attendeeId: bookingSeat?.attendeeId }) })
        .build();
      if (!updatedEvt) {
        throw new HttpError({
          statusCode: 400,
          message: "Failed to build event with new identifiers due to missing required fields",
        });
      }
      evt = updatedEvt;
    }
  }

  if (reqBody.recurringEventId && eventType.recurringEvent) {
    eventType.recurringEvent = Object.assign({}, eventType.recurringEvent, { count: recurringCount });
    evt.recurringEvent = eventType.recurringEvent;
  }

  const changedOrganizer =
    !!originalRescheduledBooking &&
    eventType.schedulingType === SchedulingType.ROUND_ROBIN &&
    originalRescheduledBooking.userId !== evt.organizer.id;

  const skipDeleteEventsAndMeetings = changedOrganizer;

  const isBookingRequestedReschedule =
    !!originalRescheduledBooking && !!originalRescheduledBooking.rescheduled && originalRescheduledBooking.status === BookingStatus.CANCELLED;

  if (changedOrganizer && originalRescheduledBooking?.user?.name && organizerUser?.name) {
    evt.title = updateHostInEventName(
      originalRescheduledBooking.title,
      originalRescheduledBooking.user.name,
      organizerUser.name
    );
  }

  let results: EventResult<AdditionalInformation & { url?: string; iCalUID?: string }>[] = [];
  let referencesToCreate: PartialReference[] = [];

  let booking: CreatedBooking | null = null;

  loggerWithEventDetails.debug(
    "Going to create booking in DB now",
    safeStringify({
      organizerUser: organizerUser.id,
      attendeesList: attendeesList.map((g) => ({ timeZone: g.timeZone })),
      requiresConfirmation: evt.requiresConfirmation,
      isConfirmedByDefault,
      userReschedulingIsOwner,
    })
  );

  let assignmentReason: { reasonEnum: AssignmentReasonEnum; reasonString: string } | undefined;

  try {
    if (!isDryRun) {
      booking = await createBooking({
        uid,
        rescheduledBy: reqBody.rescheduledBy,
        routingFormResponseId: routingFormResponseId,
        reroutingFormResponses: reroutingFormResponses ?? null,
        reqBody: { user: reqBody.user, metadata: reqBody.metadata, recurringEventId: reqBody.recurringEventId },
        eventType: {
          eventTypeData: eventType,
          id: eventTypeId,
          slug: eventTypeSlug,
          organizerUser,
          isConfirmedByDefault,
          paymentAppData,
        },
        input: { bookerEmail, rescheduleReason, smsReminderNumber, responses },
        evt,
        originalRescheduledBooking,
        creationSource: input.bookingData.creationSource,
        tracking: reqBody.tracking,
      });

      if (booking?.userId) {
        const usersRepository = new UsersRepository();
        await usersRepository.updateLastActiveAt(booking.userId);
        const organizerUserAvailability = availableUsers.find((u) => u.id === booking?.userId);

        logger.info(`Booking created`, {
          bookingUid: booking.uid,
          selectedCalendarIds: organizerUser.allSelectedCalendars?.map((c) => c.id) ?? [],
          availabilitySnapshot: organizerUserAvailability?.availabilityData
            ? formatAvailabilitySnapshot(organizerUserAvailability.availabilityData)
            : null,
        });
      }

      if (eventType.schedulingType === SchedulingType.ROUND_ROBIN) {
        if (reqBody.crmOwnerRecordType && reqBody.crmAppSlug && contactOwnerEmail && routingFormResponseId) {
          assignmentReason = await AssignmentReasonRecorder.CRMOwnership({
            bookingId: booking.id,
            crmAppSlug: reqBody.crmAppSlug,
            teamMemberEmail: contactOwnerEmail,
            recordType: reqBody.crmOwnerRecordType,
            routingFormResponseId,
            recordId: crmRecordId,
          });
        } else if (routingFormResponseId && teamId) {
          assignmentReason = await AssignmentReasonRecorder.routingFormRoute({
            bookingId: booking.id,
            routingFormResponseId,
            organizerId: organizerUser.id,
            teamId,
            isRerouting: !!reroutingFormResponses,
            reroutedByEmail: reqBody.rescheduledBy,
          });
        }
      }

      const updatedEvtWithUid = CalendarEventBuilder.fromEvent(evt)?.withUid(booking.uid ?? null).build();
      if (!updatedEvtWithUid) throw new HttpError({ statusCode: 400, message: "Failed to build event with UID due to missing required fields" });
      evt = updatedEvtWithUid;

      const updatedEvtWithPassword = CalendarEventBuilder.fromEvent(evt)
        ?.withOneTimePassword(booking.oneTimePassword ?? null)
        .build();
      if (!updatedEvtWithPassword)
        throw new HttpError({
          statusCode: 400,
          message: "Failed to build event with one-time password due to missing required fields",
        });
      evt = updatedEvtWithPassword;

      if (booking && booking.id && eventType.seatsPerTimeSlot) {
        const currentAttendee = booking.attendees.find(
          (a) =>
            a.email === input.bookingData.responses.email ||
            (input.bookingData.responses.attendeePhoneNumber && a.phoneNumber === input.bookingData.responses.attendeePhoneNumber)
        );

        const uniqueAttendeeId = uuid();
        await prisma.bookingSeat.create({
          data: {
            referenceUid: uniqueAttendeeId,
            data: { description: additionalNotes, responses },
            metadata: reqBody.metadata,
            booking: { connect: { id: booking.id } },
            attendee: { connect: { id: currentAttendee?.id } },
          },
        });
        evt.attendeeSeatId = uniqueAttendeeId;
      }
    } else {
      const { booking: dryRunBooking, troubleshooterData: _troubleshooterData } = buildDryRunBooking({
        eventTypeId,
        organizerUser,
        eventName,
        startTime: reqBody.start,
        endTime: reqBody.end,
        contactOwnerFromReq,
        contactOwnerEmail,
        allHostUsers: users,
        isManagedEventType,
      });

      booking = dryRunBooking;
      troubleshooterData = { ...troubleshooterData, ..._troubleshooterData };
    }
  } catch (_err) {
    const err = getErrorFromUnknown(_err);
    loggerWithEventDetails.error(`Booking ${eventTypeId} failed`, "Error when saving booking to db", err.message);
    if ((err as any).code === "P2002") {
      throw new HttpError({ statusCode: 409, message: ErrorCode.BookingConflict });
    }
    throw err;
  }

  const credentials = await refreshCredentials(allCredentials);
  const apps = eventTypeAppMetadataOptionalSchema.parse(eventType?.metadata?.apps);
  const eventManager = !isDryRun ? new EventManager({ ...organizerUser, credentials }, apps) : buildDryRunEventManager();

  let videoCallUrl: string | null | undefined;

  // --- RESCHEDULE PATH (still synchronous, but moves emails/etc to background) ---
  if (!eventType.seatsPerTimeSlot && originalRescheduledBooking?.uid) {
    log.silly("Rescheduling booking", originalRescheduledBooking.uid);
    await WorkflowRepository.deleteAllWorkflowReminders(originalRescheduledBooking.workflowReminders);

    evt = addVideoCallDataToEvent(originalRescheduledBooking.references, evt);
    evt.rescheduledBy = reqBody.rescheduledBy;

    const previousHostDestinationCalendar = originalRescheduledBooking?.destinationCalendar
      ? [originalRescheduledBooking?.destinationCalendar]
      : [];

    if (changedOrganizer) {
      evt.videoCallData = undefined;
      evt.iCalUID = undefined;
    }

    if (changedOrganizer && originalRescheduledBooking?.user) {
      const originalHostCredentials = await getAllCredentialsIncludeServiceAccountKey(originalRescheduledBooking.user, eventType);
      const refreshedOriginalHostCredentials = await refreshCredentials(originalHostCredentials);

      const originalHostEventManager = new EventManager(
        { ...originalRescheduledBooking.user, credentials: refreshedOriginalHostCredentials },
        apps
      );

      const deletionEvent = {
        ...evt,
        organizer: {
          id: originalRescheduledBooking.user.id,
          name: originalRescheduledBooking.user.name || "",
          email: originalRescheduledBooking.user.email,
          username: originalRescheduledBooking.user.username || undefined,
          timeZone: originalRescheduledBooking.user.timeZone,
          language: { translate: tOrganizer, locale: originalRescheduledBooking.user.locale ?? "en" },
          timeFormat: getTimeFormatStringFromUserTimeFormat(originalRescheduledBooking.user.timeFormat),
        },
        destinationCalendar: previousHostDestinationCalendar,
        startTime: originalRescheduledBooking.startTime.toISOString(),
        endTime: originalRescheduledBooking.endTime.toISOString(),
        uid: originalRescheduledBooking.uid,
        location: originalRescheduledBooking.location,
        responses: originalRescheduledBooking.responses
          ? (originalRescheduledBooking.responses as CalEventResponses)
          : evt.responses,
      };

      await originalHostEventManager.deleteEventsAndMeetings({
        event: deletionEvent,
        bookingReferences: originalRescheduledBooking.references,
      });
    }

    const updateManager = await eventManager.reschedule(
      evt,
      originalRescheduledBooking.uid,
      undefined,
      changedOrganizer,
      previousHostDestinationCalendar,
      isBookingRequestedReschedule,
      skipDeleteEventsAndMeetings
    );

    evt.description = eventType.description;

    results = updateManager.results;
    referencesToCreate = updateManager.referencesToCreate;

    videoCallUrl = evt.videoCallData?.url ?? null;

    evt.description = eventType.description;

    const { metadata: videoMetadata, videoCallUrl: _videoCallUrl } = getVideoCallDetails({ results });

    let metadata: AdditionalInformation = {};
    metadata = videoMetadata;
    videoCallUrl = _videoCallUrl;

    const isThereAnIntegrationError = results && results.some((res) => !res.success);
    if (isThereAnIntegrationError) {
      loggerWithEventDetails.error(
        `EventManager.reschedule failure in some integrations ${organizerUser.username}`,
        safeStringify({ results })
      );
    } else {
      if (results.length) {
        if (bookingLocation === MeetLocationType) {
          const googleMeetResult = {
            appName: GoogleMeetMetadata.name,
            type: "conferencing" as const,
            uid: results[0].uid,
            originalEvent: results[0].originalEvent,
          };

          const googleCalIndex = updateManager.referencesToCreate.findIndex((ref) => ref.type === "google_calendar");
          const googleCalResult = results[googleCalIndex];

          if (!googleCalResult) {
            loggerWithEventDetails.warn("Google Calendar not installed but using Google Meet as location");
            results.push({ ...googleMeetResult, success: false, calWarnings: [tOrganizer("google_meet_warning")] });
          }

          const googleHangoutLink = Array.isArray(googleCalResult?.updatedEvent)
            ? googleCalResult.updatedEvent[0]?.hangoutLink
            : googleCalResult?.updatedEvent?.hangoutLink ?? googleCalResult?.createdEvent?.hangoutLink;

          if (googleHangoutLink) {
            results.push({ ...googleMeetResult, success: true });
            updateManager.referencesToCreate[googleCalIndex] = {
              ...updateManager.referencesToCreate[googleCalIndex],
              meetingUrl: googleHangoutLink,
            };
            updateManager.referencesToCreate.push({
              type: "google_meet_video",
              meetingUrl: googleHangoutLink,
              uid: googleCalResult.uid,
              credentialId: updateManager.referencesToCreate[googleCalIndex].credentialId,
            });
          } else if (googleCalResult && !googleHangoutLink) {
            results.push({ ...googleMeetResult, success: false });
          }
        }

        const createdOrUpdatedEvent = Array.isArray(results[0]?.updatedEvent)
          ? results[0]?.updatedEvent[0]
          : results[0]?.updatedEvent ?? results[0]?.createdEvent;

        metadata.hangoutLink = createdOrUpdatedEvent?.hangoutLink;
        metadata.conferenceData = createdOrUpdatedEvent?.conferenceData;
        metadata.entryPoints = createdOrUpdatedEvent?.entryPoints;
        evt.appsStatus = handleAppsStatus(results, booking, reqAppsStatus);
        videoCallUrl =
          metadata.hangoutLink ||
          createdOrUpdatedEvent?.url ||
          organizerOrFirstDynamicGroupMemberDefaultLocationUrl ||
          getVideoCallUrlFromCalEvent(evt) ||
          videoCallUrl;
      }

      const calendarResult = results.find((r) => r.type.includes("_calendar"));
      evt.iCalUID = Array.isArray(calendarResult?.updatedEvent)
        ? calendarResult?.updatedEvent[0]?.iCalUID
        : calendarResult?.updatedEvent?.iCalUID || undefined;
    }

    evt.appsStatus = handleAppsStatus(results, booking, reqAppsStatus);

    // move emails to background
    if (noEmail !== true && isConfirmedByDefault) {
      const copyEvent = cloneDeep(evt);
      const copyEventAdditionalInfo = {
        ...copyEvent,
        additionalInformation: metadata,
        additionalNotes,
        cancellationReason: `$RCH$${rescheduleReason ? rescheduleReason : ""}`,
      };
      const cancelledRRHostEvt = cloneDeep(copyEventAdditionalInfo);

      fireAndForget(async () => {
        if (eventType.schedulingType === SchedulingType.ROUND_ROBIN) {
          const originalBookingMemberEmails: Person[] = [];
          for (const u of originalRescheduledBooking.attendees) {
            const translate = await getT(u.locale);
            originalBookingMemberEmails.push({
              name: u.name,
              email: u.email,
              timeZone: u.timeZone,
              phoneNumber: u.phoneNumber,
              language: { translate, locale: u.locale ?? "en" },
            });
          }
          if (originalRescheduledBooking.user) {
            const translate = await getT(originalRescheduledBooking.user.locale);
            const originalOrganizer = originalRescheduledBooking.user;
            originalBookingMemberEmails.push({
              ...originalRescheduledBooking.user,
              username: originalRescheduledBooking.user.username ?? undefined,
              timeFormat: getTimeFormatStringFromUserTimeFormat(originalRescheduledBooking.user.timeFormat),
              name: originalRescheduledBooking.user.name || "",
              language: { translate, locale: originalRescheduledBooking.user.locale ?? "en" },
            });

            if (changedOrganizer) {
              cancelledRRHostEvt.title = originalRescheduledBooking.title;
              cancelledRRHostEvt.startTime = dayjs(originalRescheduledBooking?.startTime).utc().format() || copyEventAdditionalInfo.startTime;
              cancelledRRHostEvt.endTime = dayjs(originalRescheduledBooking?.endTime).utc().format() || copyEventAdditionalInfo.endTime;
              cancelledRRHostEvt.organizer = {
                email: originalOrganizer.email,
                name: originalOrganizer.name || "",
                timeZone: originalOrganizer.timeZone,
                language: { translate, locale: originalOrganizer.locale || "en" },
              };
            }
          }

          const newBookingMemberEmails: Person[] =
            copyEvent.team?.members.map((m) => m).concat(copyEvent.organizer).concat(copyEvent.attendees) || [];

          const match = (a: Person, b: Person) => a.email === b.email;

          const newBookedMembers = newBookingMemberEmails.filter(
            (m) => !originalBookingMemberEmails.find((o) => match(o, m))
          );
          const cancelledMembers = originalBookingMemberEmails.filter(
            (m) => !newBookingMemberEmails.find((n) => match(m, n))
          );
          const rescheduledMembers = newBookingMemberEmails.filter((m) =>
            originalBookingMemberEmails.find((o) => match(o, m))
          );

          sendRoundRobinRescheduledEmailsAndSMS({ ...copyEventAdditionalInfo, iCalUID }, rescheduledMembers, eventType.metadata);
          sendRoundRobinScheduledEmailsAndSMS({
            calEvent: copyEventAdditionalInfo,
            members: newBookedMembers,
            eventTypeMetadata: eventType.metadata,
          });
          sendRoundRobinCancelledEmailsAndSMS(cancelledRRHostEvt, cancelledMembers, eventType.metadata);
        } else {
          await sendRescheduledEmailsAndSMS(
            {
              ...copyEvent,
              additionalInformation: metadata,
              additionalNotes,
              cancellationReason: `$RCH$${rescheduleReason ? rescheduleReason : ""}`,
            },
            eventType?.metadata
          );
        }
      });
    }
  }
  // --- CREATE PATH (optimize by backgrounding heavy post-work) ---
  else if (isConfirmedByDefault) {
    if (!booking) throw new HttpError({ statusCode: 400, message: "Booking failed" });

    const bookingRequiresPaymentNow =
      !Number.isNaN(paymentAppData.price) && paymentAppData.price > 0 && !originalRescheduledBooking?.paid && !!booking;

    if (!bookingRequiresPaymentNow) {
      const immediateResponse = {
        ...booking,
        user: { ...booking.user, email: null },
        paymentRequired: false,
      };

      // background: create events, emails, references, webhooks, reminders, analytics
      fireAndForget(async () => {
        try {
          const createManager = areCalendarEventsEnabled ? await eventManager.create(evt) : placeholderCreatedEvent;
          if (evt.location) booking.location = evt.location;
          evt.description = eventType.description;

          const bgResults = createManager.results;
          const bgReferencesToCreate = createManager.referencesToCreate;

          const additionalInformation: AdditionalInformation = {};
          let bgVideoCallUrl: string | undefined;

          if (bgResults.length) {
            if (bookingLocation === MeetLocationType) {
              const googleMeetResult = {
                appName: GoogleMeetMetadata.name,
                type: "conferencing" as const,
                uid: bgResults[0].uid,
                originalEvent: bgResults[0].originalEvent,
              };

              const googleCalIndex = bgReferencesToCreate.findIndex((ref) => ref.type === "google_calendar");
              const googleCalResult = bgResults[googleCalIndex];

              if (!googleCalResult) {
                log.warn("Google Calendar not installed but using Google Meet as location");
                bgResults.push({ ...googleMeetResult, success: false, calWarnings: [tOrganizer("google_meet_warning")] });
              }

              if (googleCalResult?.createdEvent?.hangoutLink) {
                bgResults.push({ ...googleMeetResult, success: true });
                bgReferencesToCreate[googleCalIndex] = {
                  ...bgReferencesToCreate[googleCalIndex],
                  meetingUrl: googleCalResult.createdEvent.hangoutLink,
                };
                bgReferencesToCreate.push({
                  type: "google_meet_video",
                  meetingUrl: googleCalResult.createdEvent.hangoutLink,
                  uid: googleCalResult.uid,
                  credentialId: bgReferencesToCreate[googleCalIndex].credentialId,
                });
              } else if (googleCalResult && !googleCalResult.createdEvent?.hangoutLink) {
                bgResults.push({ ...googleMeetResult, success: false });
              }
            }

            additionalInformation.hangoutLink = bgResults[0].createdEvent?.hangoutLink;
            additionalInformation.conferenceData = bgResults[0].createdEvent?.conferenceData;
            additionalInformation.entryPoints = bgResults[0].createdEvent?.entryPoints;

            evt.appsStatus = handleAppsStatus(bgResults, booking, reqAppsStatus);
            bgVideoCallUrl =
              additionalInformation.hangoutLink || organizerOrFirstDynamicGroupMemberDefaultLocationUrl || bgVideoCallUrl;

            if (!isDryRun && evt.iCalUID !== booking.iCalUID) {
              await prisma.booking.update({
                where: { id: booking.id },
                data: { iCalUID: evt.iCalUID || booking.iCalUID },
              });
            }
          }

          if (noEmail !== true && !(eventType.seatsPerTimeSlot && rescheduleUid)) {
            const wf = await loadWorkflows();
            await sendScheduledEmailsAndSMS(
              { ...evt, additionalInformation, additionalNotes, customInputs },
              eventNameObject,
              eventType.metadata?.disableStandardEmails?.confirmation?.host
                ? allowDisablingHostConfirmationEmails(wf)
                : false,
              eventType.metadata?.disableStandardEmails?.confirmation?.attendee
                ? allowDisablingAttendeeConfirmationEmails(wf)
                : false,
              eventType.metadata
            );
          }

          if (booking.location?.startsWith("http")) bgVideoCallUrl = booking.location;
          const metadata = bgVideoCallUrl ? { videoCallUrl: getVideoCallUrlFromCalEvent(evt) || bgVideoCallUrl } : undefined;

          if (!isDryRun) {
            await prisma.booking.update({
              where: { uid: booking.uid },
              data: {
                location: evt.location,
                metadata: { ...(typeof booking.metadata === "object" && booking.metadata), ...metadata },
                references: { createMany: { data: bgReferencesToCreate } },
              },
            });
          }

          const [subscribersMeetingEnded, subscribersMeetingStarted] = await Promise.all([
            getWebhooks(subscriberOptionsMeetingEnded),
            getWebhooks(subscriberOptionsMeetingStarted),
          ]);

          const scheduleTriggerPromises: Promise<unknown>[] = [];
          if (booking && booking.status === BookingStatus.ACCEPTED) {
            const bookingWithCalEventResponses = { ...booking, responses: reqBody.calEventResponses };
            for (const subscriber of subscribersMeetingEnded) {
              scheduleTriggerPromises.push(
                scheduleTrigger({
                  booking: bookingWithCalEventResponses,
                  subscriberUrl: subscriber.subscriberUrl,
                  subscriber,
                  triggerEvent: WebhookTriggerEvents.MEETING_ENDED,
                  isDryRun,
                })
              );
            }
            for (const subscriber of subscribersMeetingStarted) {
              scheduleTriggerPromises.push(
                scheduleTrigger({
                  booking: bookingWithCalEventResponses,
                  subscriberUrl: subscriber.subscriberUrl,
                  subscriber,
                  triggerEvent: WebhookTriggerEvents.MEETING_STARTED,
                  isDryRun,
                })
              );
            }
          }
          await Promise.allSettled(scheduleTriggerPromises);

          const webhookData: EventPayloadType = {
            ...evt,
            ...eventTypeInfo,
            bookingId: booking?.id,
            rescheduleId: undefined,
            rescheduleUid: undefined,
            rescheduleStartTime: undefined,
            rescheduleEndTime: undefined,
            metadata: { ...(metadata || {}), ...reqBody.metadata },
            eventTypeId,
            status: "ACCEPTED",
            smsReminderNumber: booking?.smsReminderNumber || undefined,
            rescheduledBy: reqBody.rescheduledBy,
          };

          await handleWebhookTrigger({ subscriberOptions, eventTrigger, webhookData, isDryRun });

          const evtWithMetadata = {
            ...evt,
            rescheduleReason,
            metadata,
            eventType: { slug: eventType.slug, schedulingType: eventType.schedulingType, hosts: eventType.hosts },
            bookerUrl,
          };

          const wf = await loadWorkflows();

          if (!eventType.metadata?.disableStandardEmails?.all?.attendee) {
            await scheduleMandatoryReminder({
              evt: evtWithMetadata,
              workflows: wf,
              requiresConfirmation: !isConfirmedByDefault,
              hideBranding: !!eventType.owner?.hideBranding,
              seatReferenceUid: evt.attendeeSeatId,
              isPlatformNoEmail: noEmail && Boolean(platformClientId),
              isDryRun,
            });
          }

          await scheduleWorkflowReminders({
            workflows: wf,
            smsReminderNumber: smsReminderNumber || null,
            calendarEvent: evtWithMetadata,
            isNotConfirmed: false,
            isRescheduleEvent: false,
            isFirstRecurringEvent: input.bookingData.allRecurringDates
              ? input.bookingData.isFirstRecurringSlot
              : undefined,
            hideBranding: !!eventType.owner?.hideBranding,
            seatReferenceUid: evt.attendeeSeatId,
            isDryRun,
          });

          await scheduleNoShowTriggers({
            booking: { startTime: booking.startTime, id: booking.id, location: booking.location },
            triggerForUser,
            organizerUser: { id: organizerUser.id },
            eventTypeId,
            teamId,
            orgId,
            isDryRun,
          });

          if (!isDryRun) {
            await handleAnalyticsEvents({
              credentials: allCredentials,
              rawBookingData,
              bookingInfo: { name: fullName, email: bookerEmail, eventName: "Cal.com lead" },
              isTeamEventType,
            });
          }
        } catch (e) {
          loggerWithEventDetails.error("Background work failed (fast-ack path)", safeStringify({ e }));
        }
      });

      return {
        ...immediateResponse,
        ...luckyUserResponse,
        isDryRun,
        ...(isDryRun ? { troubleshooterData } : {}),
        references: [],
        seatReferenceUid: evt.attendeeSeatId,
        videoCallUrl: undefined,
      };
    }

    // Payment required => keep synchronous create so we can return payment ids
    const createManager = areCalendarEventsEnabled ? await eventManager.create(evt) : placeholderCreatedEvent;
    if (evt.location) booking.location = evt.location;
    evt.description = eventType.description;

    results = createManager.results;
    referencesToCreate = createManager.referencesToCreate;
    videoCallUrl = evt.videoCallData?.url ?? null;

    if (results.length > 0 && results.every((res) => !res.success)) {
      loggerWithEventDetails.error(
        `EventManager.create failure in some integrations ${organizerUser.username}`,
        safeStringify({ results })
      );
    } else {
      const additionalInformation: AdditionalInformation = {};
      if (results.length) {
        if (bookingLocation === MeetLocationType) {
          const googleMeetResult = {
            appName: GoogleMeetMetadata.name,
            type: "conferencing" as const,
            uid: results[0].uid,
            originalEvent: results[0].originalEvent,
          };

          const googleCalIndex = referencesToCreate.findIndex((ref) => ref.type === "google_calendar");
          const googleCalResult = results[googleCalIndex];

          if (!googleCalResult) {
            loggerWithEventDetails.warn("Google Calendar not installed but using Google Meet as location");
            results.push({ ...googleMeetResult, success: false, calWarnings: [tOrganizer("google_meet_warning")] });
          }

          if (googleCalResult?.createdEvent?.hangoutLink) {
            results.push({ ...googleMeetResult, success: true });
            referencesToCreate[googleCalIndex] = {
              ...referencesToCreate[googleCalIndex],
              meetingUrl: googleCalResult.createdEvent.hangoutLink,
            };
            referencesToCreate.push({
              type: "google_meet_video",
              meetingUrl: googleCalResult.createdEvent.hangoutLink,
              uid: googleCalResult.uid,
              credentialId: referencesToCreate[googleCalIndex].credentialId,
            });
          } else if (googleCalResult && !googleCalResult.createdEvent?.hangoutLink) {
            results.push({ ...googleMeetResult, success: false });
          }
        }

        additionalInformation.hangoutLink = results[0].createdEvent?.hangoutLink;
        additionalInformation.conferenceData = results[0].createdEvent?.conferenceData;
        additionalInformation.entryPoints = results[0].createdEvent?.entryPoints;
        evt.appsStatus = handleAppsStatus(results, booking, reqAppsStatus);
        videoCallUrl = additionalInformation.hangoutLink || organizerOrFirstDynamicGroupMemberDefaultLocationUrl || videoCallUrl;

        if (!isDryRun && evt.iCalUID !== booking.iCalUID) {
          await prisma.booking.update({
            where: { id: booking.id },
            data: { iCalUID: evt.iCalUID || booking.iCalUID },
          });
        }
      }

      if (noEmail !== true) {
        const wf = await loadWorkflows();
        const isHostDisabled = eventType.metadata?.disableStandardEmails?.confirmation?.host
          ? allowDisablingHostConfirmationEmails(wf)
          : false;
        const isAttendeeDisabled = eventType.metadata?.disableStandardEmails?.confirmation?.attendee
          ? allowDisablingAttendeeConfirmationEmails(wf)
          : false;

        loggerWithEventDetails.debug(
          "Emails: Sending scheduled emails for booking confirmation",
          safeStringify({ calEvent: getPiiFreeCalendarEvent(evt) })
        );

        if (!isDryRun && !(eventType.seatsPerTimeSlot && rescheduleUid)) {
          await sendScheduledEmailsAndSMS(
            { ...evt, additionalInformation, additionalNotes, customInputs },
            eventNameObject,
            isHostDisabled,
            isAttendeeDisabled,
            eventType.metadata
          );
        }
      }
    }
  } else {
    // Pending bookings (confirmation required) — background the emails
    loggerWithEventDetails.debug(
      `EventManager doesn't need to create or reschedule event for booking ${organizerUser.username}`,
      safeStringify({
        calEvent: getPiiFreeCalendarEvent(evt),
        isConfirmedByDefault,
        paymentValue: paymentAppData.price,
      })
    );
  }

  const bookingRequiresPayment =
    !Number.isNaN(paymentAppData.price) && paymentAppData.price > 0 && !originalRescheduledBooking?.paid && !!booking;

  if (!isConfirmedByDefault && noEmail !== true && !bookingRequiresPayment) {
    fireAndForget(async () => {
      await sendOrganizerRequestEmail({ ...evt, additionalNotes }, eventType.metadata);
      await sendAttendeeRequestEmailAndSMS({ ...evt, additionalNotes }, attendeesList[0], eventType.metadata);
    });
  }

  if (booking.location?.startsWith("http")) videoCallUrl = booking.location;

  const metadata = videoCallUrl ? { videoCallUrl: getVideoCallUrlFromCalEvent(evt) || videoCallUrl } : undefined;

  const webhookData: EventPayloadType = {
    ...evt,
    ...eventTypeInfo,
    bookingId: booking?.id,
    rescheduleId: originalRescheduledBooking?.id || undefined,
    rescheduleUid,
    rescheduleStartTime: originalRescheduledBooking?.startTime
      ? dayjs(originalRescheduledBooking?.startTime).utc().format()
      : undefined,
    rescheduleEndTime: originalRescheduledBooking?.endTime
      ? dayjs(originalRescheduledBooking?.endTime).utc().format()
      : undefined,
    metadata: { ...metadata, ...reqBody.metadata },
    eventTypeId,
    status: "ACCEPTED",
    smsReminderNumber: booking?.smsReminderNumber || undefined,
    rescheduledBy: reqBody.rescheduledBy,
    ...(assignmentReason ? { assignmentReason: [assignmentReason] } : {}),
  };

  if (bookingRequiresPayment) {
    loggerWithEventDetails.debug(`Booking ${organizerUser.username} requires payment`);
    const credentialPaymentAppCategories = await prisma.credential.findMany({
      where: {
        ...(paymentAppData.credentialId ? { id: paymentAppData.credentialId } : { userId: organizerUser.id }),
        app: { categories: { hasSome: ["payment"] } },
      },
      select: { key: true, appId: true, app: { select: { categories: true, dirName: true } } },
    });
    const eventTypePaymentAppCredential = credentialPaymentAppCategories.find((c) => c.appId === paymentAppData.appId);
    if (!eventTypePaymentAppCredential) {
      throw new HttpError({ statusCode: 400, message: "Missing payment credentials" });
    }

    if (!booking.user) booking.user = organizerUser;
    const payment = await handlePayment({
      evt,
      selectedEventType: eventType,
      paymentAppCredentials: eventTypePaymentAppCredential as IEventTypePaymentCredentialType,
      booking,
      bookerName: fullName,
      bookerEmail,
      bookerPhoneNumber,
      isDryRun,
    });

    const subscriberOptionsPaymentInitiated: GetSubscriberOptions = {
      userId: triggerForUser ? organizerUser.id : null,
      eventTypeId,
      triggerEvent: WebhookTriggerEvents.BOOKING_PAYMENT_INITIATED,
      teamId,
      orgId,
      oAuthClientId: platformClientId,
    };
    fireAndForget(async () => {
      await handleWebhookTrigger({
        subscriberOptions: subscriberOptionsPaymentInitiated,
        eventTrigger: WebhookTriggerEvents.BOOKING_PAYMENT_INITIATED,
        webhookData: { ...webhookData, paymentId: payment?.id },
        isDryRun,
      });
    });

    const bookingResponse = {
      ...booking,
      user: { ...booking.user, email: null },
      videoCallUrl: metadata?.videoCallUrl,
      seatReferenceUid: evt.attendeeSeatId,
    };

    return {
      ...bookingResponse,
      ...luckyUserResponse,
      message: "Payment required",
      paymentRequired: true,
      paymentUid: payment?.uid,
      paymentId: payment?.id,
      isDryRun,
      ...(isDryRun ? { troubleshooterData } : {}),
    };
  }

  loggerWithEventDetails.debug(`Booking ${organizerUser.username} completed`);

  // schedule/cancel webhook triggers & send created/rescheduled webhook in background to reduce tail-latency
  fireAndForget(async () => {
    try {
      const subscribersMeetingEnded = await getWebhooks(subscriberOptionsMeetingEnded);
      const subscribersMeetingStarted = await getWebhooks(subscriberOptionsMeetingStarted);

      let deleteWebhookScheduledTriggerPromise: Promise<unknown> = Promise.resolve();
      const scheduleTriggerPromises: Promise<unknown>[] = [];

      if (rescheduleUid && originalRescheduledBooking) {
        deleteWebhookScheduledTriggerPromise = deleteWebhookScheduledTriggers({
          booking: originalRescheduledBooking,
          isDryRun,
        });
      }

      if (booking && booking.status === BookingStatus.ACCEPTED) {
        const bookingWithCalEventResponses = { ...booking, responses: reqBody.calEventResponses };
        for (const subscriber of subscribersMeetingEnded) {
          scheduleTriggerPromises.push(
            scheduleTrigger({
              booking: bookingWithCalEventResponses,
              subscriberUrl: subscriber.subscriberUrl,
              subscriber,
              triggerEvent: WebhookTriggerEvents.MEETING_ENDED,
              isDryRun,
            })
          );
        }
        for (const subscriber of subscribersMeetingStarted) {
          scheduleTriggerPromises.push(
            scheduleTrigger({
              booking: bookingWithCalEventResponses,
              subscriberUrl: subscriber.subscriberUrl,
              subscriber,
              triggerEvent: WebhookTriggerEvents.MEETING_STARTED,
              isDryRun,
            })
          );
        }
      }

      await Promise.allSettled([deleteWebhookScheduledTriggerPromise, ...scheduleTriggerPromises]);

      await handleWebhookTrigger({ subscriberOptions, eventTrigger, webhookData, isDryRun });
    } catch (error) {
      loggerWithEventDetails.error("Error while scheduling or canceling webhook triggers", JSON.stringify({ error }));
    }
  });

  try {
    const hashedLinkService = new HashedLinkService();
    if (hasHashedBookingLink && reqBody.hashedLink && !isDryRun) {
      await hashedLinkService.validateAndIncrementUsage(reqBody.hashedLink as string);
    }
  } catch (error) {
    loggerWithEventDetails.error("Error while updating hashed link", JSON.stringify({ error }));
    if (error instanceof Error) {
      throw new HttpError({ statusCode: 410, message: error.message });
    }
    throw new HttpError({ statusCode: 500, message: "Failed to process booking link" });
  }

  if (!booking) throw new HttpError({ statusCode: 400, message: "Booking failed" });

  // defer heavy DB reference writes to background to reduce response time
  fireAndForget(async () => {
    try {
      if (!isDryRun) {
        await prisma.booking.update({
          where: { uid: booking!.uid },
          data: {
            location: evt.location,
            metadata: { ...(typeof booking!.metadata === "object" && booking!.metadata), ...(metadata || {}) },
            references: { createMany: { data: referencesToCreate } },
          },
        });
      }
    } catch (error) {
      loggerWithEventDetails.error("Error while creating booking references", JSON.stringify({ error }));
    }
  });

  const evtWithMetadata = {
    ...evt,
    rescheduleReason,
    metadata,
    eventType: { slug: eventType.slug, schedulingType: eventType.schedulingType, hosts: eventType.hosts },
    bookerUrl,
  };

  fireAndForget(async () => {
    try {
      const wf = await loadWorkflows();
      if (!eventType.metadata?.disableStandardEmails?.all?.attendee) {
        await scheduleMandatoryReminder({
          evt: evtWithMetadata,
          workflows: wf,
          requiresConfirmation: !isConfirmedByDefault,
          hideBranding: !!eventType.owner?.hideBranding,
          seatReferenceUid: evt.attendeeSeatId,
          isPlatformNoEmail: noEmail && Boolean(platformClientId),
          isDryRun,
        });
      }

      await scheduleWorkflowReminders({
        workflows: wf,
        smsReminderNumber: smsReminderNumber || null,
        calendarEvent: evtWithMetadata,
        isNotConfirmed: rescheduleUid ? false : !isConfirmedByDefault,
        isRescheduleEvent: !!rescheduleUid,
        isFirstRecurringEvent: input.bookingData.allRecurringDates ? input.bookingData.isFirstRecurringSlot : undefined,
        hideBranding: !!eventType.owner?.hideBranding,
        seatReferenceUid: evt.attendeeSeatId,
        isDryRun,
      });

      if (isConfirmedByDefault) {
        await scheduleNoShowTriggers({
          booking: { startTime: booking!.startTime, id: booking!.id, location: booking!.location },
          triggerForUser,
          organizerUser: { id: organizerUser.id },
          eventTypeId,
          teamId,
          orgId,
          isDryRun,
        });
      }

      if (!isDryRun) {
        await handleAnalyticsEvents({
          credentials: allCredentials,
          rawBookingData,
          bookingInfo: { name: fullName, email: bookerEmail, eventName: "Cal.com lead" },
          isTeamEventType,
        });
      }
    } catch (error) {
      loggerWithEventDetails.error("Post-processing background tasks failed", JSON.stringify({ error }));
    }
  });

  const bookingResponse = {
    ...booking,
    user: { ...booking.user, email: null },
    paymentRequired: false,
  };

  return {
    ...bookingResponse,
    ...luckyUserResponse,
    isDryRun,
    ...(isDryRun ? { troubleshooterData } : {}),
    references: referencesToCreate,
    seatReferenceUid: evt.attendeeSeatId,
    videoCallUrl: metadata?.videoCallUrl,
  };
}

export default handler;
