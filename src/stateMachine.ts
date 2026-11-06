// The payment lifecycle as data. Anything not in this table is illegal.

export type PaymentStatus =
  | "authorizing"
  | "authorized"
  | "partially_captured"
  | "captured"
  | "partially_refunded"
  | "refunded"
  | "voided"
  | "expired"
  | "declined"
  | "failed";

export type PaymentEvent =
  | "approve"
  | "decline"
  | "fail"
  | "capture_partial"
  | "capture_full"
  | "void"
  | "expire"
  | "refund_partial"
  | "refund_full";

export const STATUSES: PaymentStatus[] = [
  "authorizing",
  "authorized",
  "partially_captured",
  "captured",
  "partially_refunded",
  "refunded",
  "voided",
  "expired",
  "declined",
  "failed",
];

export const EVENTS: PaymentEvent[] = [
  "approve",
  "decline",
  "fail",
  "capture_partial",
  "capture_full",
  "void",
  "expire",
  "refund_partial",
  "refund_full",
];

export const TRANSITIONS: Record<PaymentStatus, Partial<Record<PaymentEvent, PaymentStatus>>> = {
  authorizing: { approve: "authorized", decline: "declined", fail: "failed" },
  authorized: { capture_partial: "partially_captured", capture_full: "captured", void: "voided", expire: "expired" },
  // Voiding or expiring a partially captured auth gives back the remainder; what was captured stands.
  partially_captured: { capture_partial: "partially_captured", capture_full: "captured", void: "captured", expire: "captured" },
  captured: { refund_partial: "partially_refunded", refund_full: "refunded" },
  partially_refunded: { refund_partial: "partially_refunded", refund_full: "refunded" },
  refunded: {},
  voided: {},
  expired: {},
  declined: {},
  failed: {},
};

export class IllegalTransition extends Error {
  name = "IllegalTransition";
}

export function transition(status: PaymentStatus, event: PaymentEvent): PaymentStatus {
  const next = TRANSITIONS[status][event];
  if (!next) throw new IllegalTransition(`${event} is not allowed from ${status}`);
  return next;
}

export const isTerminal = (status: PaymentStatus) => Object.keys(TRANSITIONS[status]).length === 0;

export type Command = "capture" | "void" | "refund";

const COMMAND_EVENT: Record<Command, PaymentEvent> = { capture: "capture_partial", void: "void", refund: "refund_partial" };

// Can a merchant even ask for this right now? Amount rules are checked separately, under the payment's row lock.
export function canAdmit(status: PaymentStatus, command: Command): boolean {
  return TRANSITIONS[status][COMMAND_EVENT[command]] !== undefined;
}
