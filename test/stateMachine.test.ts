import { describe, expect, it } from "vitest";
import { canAdmit, EVENTS, IllegalTransition, isTerminal, STATUSES, transition, TRANSITIONS } from "../src/stateMachine.ts";

// Written out by hand rather than derived from TRANSITIONS: this is the spec the table is checked against.
const LEGAL = new Set([
  "authorizing/approve",
  "authorizing/decline",
  "authorizing/fail",
  "authorized/capture_partial",
  "authorized/capture_full",
  "authorized/void",
  "authorized/expire",
  "partially_captured/capture_partial",
  "partially_captured/capture_full",
  "partially_captured/void",
  "partially_captured/expire",
  "captured/refund_partial",
  "captured/refund_full",
  "partially_refunded/refund_partial",
  "partially_refunded/refund_full",
]);

describe("payment state machine", () => {
  for (const status of STATUSES) {
    for (const event of EVENTS) {
      const legal = LEGAL.has(`${status}/${event}`);
      it(`${status} + ${event} is ${legal ? "legal" : "illegal"}`, () => {
        if (legal) expect(STATUSES).toContain(transition(status, event));
        else expect(() => transition(status, event)).toThrow(IllegalTransition);
      });
    }
  }

  it("a partial capture closed by void or expiry ends up captured, not voided", () => {
    expect(transition("partially_captured", "void")).toBe("captured");
    expect(transition("partially_captured", "expire")).toBe("captured");
  });

  it("terminal states have no way out", () => {
    for (const s of ["refunded", "voided", "expired", "declined", "failed"] as const) {
      expect(isTerminal(s)).toBe(true);
      expect(TRANSITIONS[s]).toEqual({});
    }
  });

  it("merchants can only issue commands the table allows", () => {
    expect(canAdmit("authorized", "capture")).toBe(true);
    expect(canAdmit("captured", "capture")).toBe(false);
    expect(canAdmit("captured", "void")).toBe(false);
    expect(canAdmit("authorized", "refund")).toBe(false);
    expect(canAdmit("partially_captured", "refund")).toBe(false);
    expect(canAdmit("voided", "refund")).toBe(false);
  });
});
