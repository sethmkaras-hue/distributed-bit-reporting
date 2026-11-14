import { describe, it, expect } from "vitest";
import { mergeMeasurements } from "./api";
describe("live reconciliation", () => {
  it("merges updates by stable SOM and parameter identity", () => {
    const old = [
      { som_id: "a", parameter: { id: "t" }, value: 1 },
      { som_id: "b", parameter: { id: "t" }, value: 2 },
    ];
    const result = mergeMeasurements(old, [
      { som_id: "a", parameter: { id: "t" }, value: 9 },
    ]);
    expect(result).toHaveLength(2);
    expect(result.find((x) => x.som_id === "a")?.value).toBe(9);
    expect(result.find((x) => x.som_id === "b")?.value).toBe(2);
  });
});
