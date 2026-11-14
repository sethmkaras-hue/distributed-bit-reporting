export interface Parameter {
  id: string;
  name: string;
  description: string;
  category: string;
  data_type: string;
  unit: string;
  collection_s: number;
  publication_s: number;
  retention_s: number;
  deadband: number;
  warning: number | null;
  critical: number | null;
}
export interface Measurement {
  som_id: string;
  boot_id: string;
  parameter: Parameter;
  value: number | string | boolean | null;
  source_time: number;
  master_time: number | null;
  cpu_time: number | null;
  validity: string;
  severity: string;
  sequence: number;
  quality: { state: string; offset_us: number; uncertainty_us: number };
}
export interface Member {
  som_id: string;
  subsystem: string;
  ip: string;
  boot_id: string;
  lifecycle: string;
  role: string;
  connection: string;
  quality?: { state: string };
  buffer?: { journal_bytes: number; overwritten: number };
}
export interface State {
  members: Member[];
  masters: Record<string, Member>;
  measurements: Measurement[];
  cpu_time: number;
  error: string | null;
  metrics: {
    source_to_cpu_p95_ms: number | null;
    archive_drops: number;
    browser_drops: number;
  };
}
export interface Event {
  som_id: string;
  boot_id: string;
  sequence: number;
  severity: string;
  description: string;
  category: string;
  source_time: number;
  state: string;
  parameter_id: string | null;
}
export interface Operation {
  id: string;
  target: string;
  kind: string;
  operator: string;
  state: string;
  error: string | null;
  requested_at: number;
  completed_at: number | null;
}
