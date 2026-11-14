export const token = () => sessionStorage.getItem("bitmesh-token") || "";
export async function api<T>(path: string, data?: unknown): Promise<T> {
  const response = await fetch(`/api/${path}`, {
    method: data === undefined ? "GET" : "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token()}`,
    },
    body: data === undefined ? undefined : JSON.stringify(data),
  });
  if (!response.ok) {
    const body = await response
      .json()
      .catch(() => ({ detail: response.statusText }));
    throw new Error(
      typeof body.detail === "string"
        ? body.detail
        : JSON.stringify(body.detail),
    );
  }
  return response.json();
}
export function mergeMeasurements<
  T extends { som_id: string; parameter: { id: string } },
>(old: T[], delta: T[]): T[] {
  const map = new Map(old.map((x) => [`${x.som_id}/${x.parameter.id}`, x]));
  delta.forEach((x) => map.set(`${x.som_id}/${x.parameter.id}`, x));
  return [...map.values()];
}
