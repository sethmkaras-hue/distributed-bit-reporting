import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  CssBaseline,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  MenuItem,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  ThemeProvider,
  Typography,
  createTheme,
} from "@mui/material";
import {
  DashboardOutlined,
  HubOutlined,
  WarningAmberOutlined,
  TerminalOutlined,
  TuneOutlined,
  Search,
  ArrowBack,
  Memory,
  Refresh,
  GraphicEq,
} from "@mui/icons-material";
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
} from "recharts";
import { api, mergeMeasurements, token } from "./api";
import type { State, Measurement, Member, Event, Operation } from "./types";
import "./style.css";
const theme = createTheme({
  palette: {
    mode: "dark",
    primary: { main: "#65dfc1" },
    background: { default: "#09121e", paper: "#111e2d" },
    text: { primary: "#e7eef7", secondary: "#8a9bb0" },
    divider: "#243347",
  },
  typography: {
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif",
    h4: { fontWeight: 650, letterSpacing: "-.04em" },
    button: { textTransform: "none", fontWeight: 600 },
  },
  shape: { borderRadius: 10 },
  components: {
    MuiTableCell: {
      styleOverrides: {
        root: { borderColor: "#243347", padding: "13px 16px" },
        head: {
          color: "#8a9bb0",
          fontSize: 11,
          textTransform: "uppercase",
          letterSpacing: ".08em",
        },
      },
    },
    MuiButton: { defaultProps: { disableElevation: true } },
    MuiChip: { styleOverrides: { root: { fontSize: 11, fontWeight: 650 } } },
  },
});
const empty: State = {
  members: [],
  masters: {},
  measurements: [],
  cpu_time: 0,
  error: null,
  metrics: { source_to_cpu_p95_ms: null, archive_drops: 0, browser_drops: 0 },
};
const pages = [
  ["overview", "Overview", DashboardOutlined],
  ["subsystem", "Subsystems", HubOutlined],
  ["faults", "Fault console", WarningAmberOutlined],
  ["operations", "Operations", TerminalOutlined],
  ["config", "Configuration", TuneOutlined],
] as const;
const format = (v: unknown) =>
  typeof v === "number" ? v.toFixed(1) : String(v ?? "—");
const time = (t: number) => new Date(t * 1000).toLocaleTimeString();
function Status({ value }: { value: string }) {
  const good = [
    "CONNECTED",
    "READY",
    "ACTIVE",
    "SUCCEEDED",
    "SYNCHRONIZED",
    "INFO",
  ].includes(value);
  const bad = ["CRITICAL", "FAILED", "DISCONNECTED", "STALE"].includes(value);
  return (
    <Chip
      size="small"
      variant="outlined"
      color={good ? "success" : bad ? "error" : "warning"}
      label={value}
    />
  );
}
function App() {
  const [state, setState] = useState<State>(empty),
    [page, setPage] = useState("overview"),
    [selected, setSelected] = useState(""),
    [subsystem, setSubsystem] = useState(""),
    [search, setSearch] = useState(""),
    [category, setCategory] = useState("All"),
    [severity, setSeverity] = useState("All"),
    [eventState, setEventState] = useState("All"),
    [eventOrder, setEventOrder] = useState("Newest"),
    [connected, setConnected] = useState(false),
    [loaded, setLoaded] = useState(false),
    [error, setError] = useState(""),
    [events, setEvents] = useState<Event[]>([]),
    [gaps, setGaps] = useState<
      { som_id: string; description: string; start?: number; end?: number }[]
    >([]),
    [operations, setOperations] = useState<Operation[]>([]),
    [acks, setAcks] = useState<
      { som_id: string; boot_id: string; sequence: number }[]
    >([]),
    [notice, setNotice] = useState(""),
    [artifact, setArtifact] = useState(""),
    [successor, setSuccessor] = useState(""),
    [confirm, setConfirm] = useState(""),
    [busy, setBusy] = useState(false),
    [config, setConfig] = useState(""),
    [auth, setAuth] = useState(token()),
    [simulation, setSimulation] = useState(false),
    [operator, setOperator] = useState({ name: "Connecting", role: "viewer" }),
    [history, setHistory] = useState<Measurement[]>([]),
    [trendParam, setTrendParam] = useState("ps.temperature"),
    [guiLatency, setGuiLatency] = useState<number | null>(null);
  const refreshArchive = async () => {
    try {
      const [e, g, c, a] = await Promise.all([
        api<Event[]>("archive/event"),
        api<typeof gaps>("archive/gap"),
        api<Operation[]>("archive/command"),
        api<typeof acks>("archive/ack"),
      ]);
      setEvents(e);
      setGaps(g);
      setOperations(c);
      setAcks(a);
    } catch (e) {
      setError(String(e));
    }
  };
  useEffect(() => {
    let active = true;
    let socket: WebSocket;
    let retry: ReturnType<typeof setTimeout>;
    let renderTimer: ReturnType<typeof setTimeout> | null = null;
    let eventTimer: ReturnType<typeof setTimeout> | null = null;
    let buffered: Measurement[] = [];
    const connect = async () => {
      try {
        const data = await api<
          State & { simulation: boolean; operator: typeof operator }
        >("state");
        if (!active) return;
        setState(data);
        setSimulation(data.simulation);
        setOperator(data.operator);
        setLoaded(true);
        setError("");
        socket = new WebSocket(
          `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/api/live`,
        );
        socket.onopen = () => {
          socket.send(JSON.stringify({ token: token() }));
          setConnected(true);
        };
        socket.onmessage = (event) => {
          const msg = JSON.parse(event.data);
          if (msg.type === "state") setState(msg.state);
          if (msg.type === "membership") {
            const stale = new Set(msg.state.stale_keys ?? []);
            const boots = new Map<string, string>(
              msg.state.members.map((node: Member) => [
                node.som_id,
                node.boot_id,
              ]),
            );
            setState((old) => ({
              ...msg.state,
              measurements: old.measurements
                .filter(
                  (m) =>
                    !boots.get(m.som_id) || boots.get(m.som_id) === m.boot_id,
                )
                .map((m) =>
                  stale.has(`${m.som_id}/${m.parameter.id}`)
                    ? { ...m, validity: "STALE" }
                    : m,
                ),
            }));
          }
          if (msg.type === "telemetry") {
            buffered.push(...msg.measurements);
            if (!renderTimer)
              renderTimer = setTimeout(() => {
                const batch = buffered;
                buffered = [];
                renderTimer = null;
                if (!active) return;
                setState((old) => ({
                  ...old,
                  measurements: mergeMeasurements(old.measurements, batch),
                }));
                const fast = batch.filter(
                  (x) =>
                    x.parameter.collection_s <= 0.2 &&
                    x.quality.state === "SYNCHRONIZED",
                );
                if (fast.length)
                  setGuiLatency(
                    Math.max(
                      ...fast.map((x) => Date.now() - x.source_time * 1000),
                    ),
                  );
              }, 100);
          }
          if (msg.type === "events" || msg.type === "command") {
            if (!eventTimer)
              eventTimer = setTimeout(() => {
                eventTimer = null;
                void refreshArchive();
              }, 500);
          }
        };
        socket.onclose = () => {
          setConnected(false);
          if (active) retry = setTimeout(connect, 1000);
        };
        socket.onerror = () => socket.close();
      } catch (e) {
        if (active) {
          setError(String(e));
          setConnected(false);
          retry = setTimeout(connect, 2000);
        }
      }
    };
    void connect();
    void refreshArchive();
    return () => {
      active = false;
      clearTimeout(retry);
      if (renderTimer) clearTimeout(renderTimer);
      if (eventTimer) clearTimeout(eventTimer);
      socket?.close();
    };
  }, []);
  useEffect(() => {
    if (selected) {
      void api<Measurement[]>(
        `archive/measurement?som_id=${selected}&limit=2000`,
      )
        .then(setHistory)
        .catch((e) => setError(String(e)));
      setConfig(
        JSON.stringify(
          state.measurements
            .filter((m) => m.som_id === selected)
            .map((m) => m.parameter),
          null,
          2,
        ),
      );
    }
  }, [selected]);
  const subsystems = [...new Set(state.members.map((n) => n.subsystem))].sort();
  const members = state.members.filter(
    (n) =>
      (!subsystem || n.subsystem === subsystem) &&
      `${n.som_id} ${n.ip} ${n.subsystem}`
        .toLowerCase()
        .includes(search.toLowerCase()),
  );
  const selectedMember = state.members.find((n) => n.som_id === selected);
  const measurements = state.measurements.filter(
    (m) =>
      (!selected || m.som_id === selected) &&
      (category === "All" || m.parameter.category === category) &&
      `${m.parameter.name} ${m.parameter.id} ${m.som_id}`
        .toLowerCase()
        .includes(search.toLowerCase()),
  );
  const measurementsBySubsystem = useMemo(() => {
    const owner = new Map(state.members.map((n) => [n.som_id, n.subsystem]));
    const groups = new Map<string, Measurement[]>();
    for (const measurement of state.measurements) {
      const subsystem = owner.get(measurement.som_id);
      if (subsystem) {
        if (!groups.has(subsystem)) groups.set(subsystem, []);
        groups.get(subsystem)!.push(measurement);
      }
    }
    return groups;
  }, [state.members, state.measurements]);
  const critical = state.measurements.filter(
    (m) => m.severity === "CRITICAL",
  ).length;
  const currentViolations =
    page === "faults"
      ? state.measurements.filter(
          (m) =>
            m.severity !== "INFO" &&
            (severity === "All" || m.severity === severity) &&
            `${m.som_id} ${m.parameter.name} ${m.parameter.id}`
              .toLowerCase()
              .includes(search.toLowerCase()),
        )
      : [];
  const navigate = (next: string) => {
    setPage(next);
    setSelected("");
    setSubsystem("");
    setSearch("");
  };
  const inspect = (som: string) => {
    setSelected(som);
    setPage("som");
    setCategory("All");
    setTrendParam("ps.temperature");
  };
  const perform = async (kind: string) => {
    if (!selectedMember) return;
    setBusy(true);
    try {
      let args: Record<string, unknown> = {};
      if (kind === "transfer") args = { successor };
      if (["program_pl", "update_software"].includes(kind)) args = { artifact };
      if (kind === "configure") args = { parameters: JSON.parse(config) };
      const result = await api<Operation>("commands", {
        id: crypto.randomUUID(),
        target: selected,
        boot_id: selectedMember.boot_id,
        kind,
        arguments: args,
      });
      setNotice(
        `${result.kind}: ${result.state}${result.error ? ` — ${result.error}` : ""}`,
      );
      setConfirm("");
      await refreshArchive();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  };
  const request = (kind: string) => {
    if (
      [
        "reset",
        "program_pl",
        "update_software",
        "approve",
        "transfer",
        "configure",
        "restart",
        "stop",
      ].includes(kind)
    )
      setConfirm(kind);
    else void perform(kind);
  };
  const inject = async (action: string, value?: string | number) => {
    try {
      await api("simulation/inject", { target: selected, action, value });
      setNotice(`Injected ${action}`);
    } catch (e) {
      setError(String(e));
    }
  };
  const chart = useMemo(
    () =>
      [
        ...history.filter((m) => m.parameter.id === trendParam),
        ...state.measurements.filter(
          (m) => m.som_id === selected && m.parameter.id === trendParam,
        ),
      ]
        .sort((a, b) => a.source_time - b.source_time)
        .slice(-120)
        .map((m) => ({
          time: time(m.source_time),
          value: typeof m.value === "number" ? m.value : null,
        })),
    [history, state.measurements, selected, trendParam],
  );
  const nodeTable = (nodes: Member[]) => (
    <TableContainer>
      <Table>
        <TableHead>
          <TableRow>
            {[
              "SOM / Address",
              "Lifecycle",
              "Link",
              "Role",
              "PTP",
              "Inspect",
            ].map((x) => (
              <TableCell key={x}>{x}</TableCell>
            ))}
          </TableRow>
        </TableHead>
        <TableBody>
          {nodes.map((n) => (
            <TableRow hover key={n.som_id}>
              <TableCell>
                <strong>{n.som_id}</strong>
                <div className="mono muted">{n.ip}</div>
              </TableCell>
              <TableCell>
                <Status value={n.lifecycle} />
              </TableCell>
              <TableCell>
                <Status value={n.connection || "DISCONNECTED"} />
              </TableCell>
              <TableCell>
                <Chip
                  size="small"
                  label={n.role}
                  color={n.role === "MASTER" ? "primary" : "default"}
                />
              </TableCell>
              <TableCell>{n.quality?.state ?? "Unknown"}</TableCell>
              <TableCell>
                <Button size="small" onClick={() => inspect(n.som_id)}>
                  Inspect →
                </Button>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
  return (
    <div className="shell">
      <aside>
        <div className="brand">
          <GraphicEq color="primary" />
          <span>
            BIT<span className="accent">Mesh</span>
            <small>ENGINEERING OPERATIONS</small>
          </span>
        </div>
        <div className="nav-label">CONTROL PLANE</div>
        <nav>
          {pages.map(([id, label, Icon]) => (
            <button
              aria-label={label}
              key={id}
              className={page === id ? "active" : ""}
              onClick={() => navigate(id)}
            >
              <Icon fontSize="small" />
              <span>{label}</span>
              {id === "faults" && critical > 0 && (
                <b className="count">{critical}</b>
              )}
            </button>
          ))}
        </nav>
        <div className="sidebar-foot">
          <span className={`dot ${connected ? "green" : "red"}`} />
          {connected ? "CPU connected" : "CPU disconnected"}
          <div className="muted">
            {simulation ? "SIMULATION ENVIRONMENT" : "CONTROL NETWORK"}
          </div>
        </div>
      </aside>
      <div className="main">
        <header className="topbar">
          <div className="breadcrumb">
            Workspace <span>/</span>{" "}
            {page === "som" ? selected : pages.find((p) => p[0] === page)?.[1]}
          </div>
          <TextField
            size="small"
            placeholder="Search SOMs, IPs, parameters…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            slotProps={{
              input: {
                startAdornment: (
                  <Search
                    fontSize="small"
                    sx={{ mr: 1, color: "text.secondary" }}
                  />
                ),
              },
            }}
          />
          <Chip
            variant="outlined"
            color={connected ? "success" : "error"}
            size="small"
            label={connected ? "LIVE" : "DISCONNECTED"}
          />
          <span className="operator">{operator.name}</span>
        </header>
        <main>
          <div className="page-heading">
            <div>
              <div className="eyebrow">DISTRIBUTED BUILT-IN TEST</div>
              <Typography variant="h4">
                {page === "som"
                  ? selected
                  : pages.find((p) => p[0] === page)?.[1]}
              </Typography>
              <p className="muted">
                {page === "overview"
                  ? "System health, subsystem authority and live diagnostics."
                  : page === "faults"
                    ? "Trace every fault from source to archive."
                    : page === "operations"
                      ? "Boot-scoped commands with fenced authority and an audit trail."
                      : page === "config"
                        ? "Inventory and category-level monitoring policy."
                        : "Processing, connectivity and BIT measurements."}
              </p>
            </div>
            <Button
              aria-label="Refresh archive"
              variant="outlined"
              startIcon={<Refresh />}
              onClick={() => void refreshArchive()}
            >
              Refresh archive
            </Button>
          </div>
          {error && (
            <Alert severity="error" onClose={() => setError("")} sx={{ mb: 2 }}>
              {error}
            </Alert>
          )}
          {state.error && <Alert severity="error">{state.error}</Alert>}
          {!connected && loaded && (
            <Alert severity="warning" sx={{ mb: 2 }}>
              Live connection lost. Values may be stale. Reconnecting
              automatically.
            </Alert>
          )}
          {!loaded && (
            <Box sx={{ p: 5, textAlign: "center" }}>
              <CircularProgress />
              <p>Connecting to CPU archive…</p>
              <TextField
                label="Access token"
                type="password"
                value={auth}
                onChange={(e) => setAuth(e.target.value)}
              />
              <Button
                onClick={() => {
                  sessionStorage.setItem("bitmesh-token", auth);
                  location.reload();
                }}
              >
                Authenticate
              </Button>
            </Box>
          )}
          {loaded && (
            <>
              {page === "overview" && (
                <>
                  <div className="stats">
                    {[
                      [
                        "SUBSYSTEMS",
                        `${Object.keys(state.masters).length} / ${subsystems.length}`,
                        "Masters connected",
                      ],
                      [
                        "SOMS ONLINE",
                        `${state.members.filter((n) => n.connection === "CONNECTED").length} / ${state.members.length}`,
                        "Inventory reachability",
                      ],
                      [
                        "CRITICAL PARAMETERS",
                        critical,
                        "Current threshold violations",
                      ],
                      [
                        "SOURCE → GUI",
                        guiLatency === null
                          ? "—"
                          : `${guiLatency.toFixed(0)} ms`,
                        "Latest fast synchronized batch",
                      ],
                    ].map(([label, value, desc]) => (
                      <section className="stat" key={label}>
                        <div className="eyebrow">{label}</div>
                        <strong>{value}</strong>
                        <span className="muted">{desc}</span>
                      </section>
                    ))}
                  </div>
                  <div className="section-heading">
                    <Typography variant="h6">Subsystem fleet</Typography>
                    <span className="muted">
                      {state.measurements.length.toLocaleString()} parameters ·
                      100 ms render batch
                    </span>
                  </div>
                  <div className="subsystem-grid">
                    {subsystems
                      .filter(
                        (s) =>
                          !search ||
                          s.toLowerCase().includes(search.toLowerCase()) ||
                          state.members.some(
                            (n) =>
                              n.subsystem === s &&
                              `${n.som_id} ${n.ip}`.includes(search),
                          ),
                      )
                      .map((s) => {
                        const ns = state.members.filter(
                          (n) => n.subsystem === s,
                        );
                        const ms = measurementsBySubsystem.get(s) ?? [];
                        const faults = ms.filter(
                          (m) => m.severity === "CRITICAL",
                        ).length;
                        const degraded =
                          ns.some(
                            (n) =>
                              n.connection !== "CONNECTED" ||
                              n.lifecycle === "QUARANTINED",
                          ) ||
                          ms.some(
                            (m) =>
                              m.severity === "WARNING" ||
                              m.validity !== "VALID",
                          );
                        const last = Math.max(
                          ...ms.map((m) => m.source_time),
                          0,
                        );
                        return (
                          <button
                            className="subsystem-card"
                            key={s}
                            onClick={() => {
                              setSubsystem(s);
                              setPage("subsystem");
                            }}
                          >
                            <div className="card-title">
                              <span className="chip-icon">
                                <HubOutlined />
                              </span>
                              <strong>{s}</strong>
                              <Status
                                value={
                                  faults
                                    ? "CRITICAL"
                                    : state.masters[s]
                                      ? degraded
                                        ? "DEGRADED"
                                        : "CONNECTED"
                                      : "DISCONNECTED"
                                }
                              />
                            </div>
                            <div className="card-metrics">
                              <div>
                                <small>ONLINE SOMS</small>
                                <strong>
                                  {
                                    ns.filter(
                                      (n) => n.connection === "CONNECTED",
                                    ).length
                                  }
                                  <span> / {ns.length}</span>
                                </strong>
                              </div>
                              <div>
                                <small>CRITICAL BITS</small>
                                <strong className={faults ? "danger" : ""}>
                                  {faults}
                                </strong>
                              </div>
                            </div>
                            <div className="capacity">
                              {ns.map((n) => (
                                <span
                                  title={n.som_id}
                                  key={n.som_id}
                                  className={
                                    n.connection === "CONNECTED"
                                      ? "green"
                                      : "red"
                                  }
                                />
                              ))}
                            </div>
                            <footer>
                              <span>
                                Master{" "}
                                <b>
                                  {state.masters[s]?.som_id ?? "No authority"}
                                </b>
                              </span>
                              <span>
                                {last ? time(last) : "No telemetry"} →
                              </span>
                            </footer>
                          </button>
                        );
                      })}
                  </div>
                  <div className="panel" style={{ marginTop: 24 }}>
                    <div className="section-heading">
                      <Typography variant="h6">
                        Recent fault activity
                      </Typography>
                      <Button onClick={() => navigate("faults")}>
                        Open console →
                      </Button>
                    </div>
                    {events.slice(0, 5).map((e) => (
                      <div
                        className="event-line"
                        key={`${e.som_id}/${e.boot_id}/${e.sequence}`}
                      >
                        <Status value={e.severity} />
                        <Button
                          onClick={() => {
                            inspect(e.som_id);
                            if (e.parameter_id) setTrendParam(e.parameter_id);
                          }}
                        >
                          {e.som_id}
                        </Button>
                        <span>{e.description}</span>
                        <time>{time(e.source_time)}</time>
                      </div>
                    ))}
                    {!events.length && (
                      <p className="muted">No archived events yet.</p>
                    )}
                  </div>
                </>
              )}
              {page === "subsystem" && (
                <>
                  <div className="toolbar">
                    <TextField
                      select
                      label="Subsystem"
                      size="small"
                      value={subsystem}
                      onChange={(e) => setSubsystem(e.target.value)}
                      sx={{ minWidth: 180 }}
                    >
                      <MenuItem value="">All subsystems</MenuItem>
                      {subsystems.map((s) => (
                        <MenuItem key={s} value={s}>
                          {s}
                        </MenuItem>
                      ))}
                    </TextField>
                    <span className="muted">
                      Master changes are archived with generation and boot
                      identity.
                    </span>
                  </div>
                  <div className="panel">{nodeTable(members)}</div>
                  <Button
                    sx={{ mt: 2 }}
                    onClick={() =>
                      void api<Member[]>("archive/leadership").then((x) =>
                        setNotice(
                          x
                            .slice(0, 10)
                            .map((n) => `${n.subsystem}: ${n.som_id}`)
                            .join(" · "),
                        ),
                      )
                    }
                  >
                    Load failover history
                  </Button>
                </>
              )}
              {page === "som" && selectedMember && (
                <>
                  <Button
                    startIcon={<ArrowBack />}
                    onClick={() => {
                      setPage("subsystem");
                      setSelected("");
                    }}
                  >
                    Back to subsystem
                  </Button>
                  <div className="som-summary panel">
                    <Memory fontSize="large" color="primary" />
                    <div>
                      <strong>{selected}</strong>
                      <div className="mono muted">
                        {selectedMember.ip} · Boot{" "}
                        {selectedMember.boot_id.slice(0, 8)}
                      </div>
                    </div>
                    <Status value={selectedMember.lifecycle} />
                    <Status value={selectedMember.connection} />
                    <Chip label={selectedMember.role} />
                    <Button
                      sx={{ ml: "auto" }}
                      variant="outlined"
                      onClick={() => setPage("operations")}
                    >
                      Control SOM
                    </Button>
                  </div>
                  <div className="panel trend">
                    <div className="section-heading">
                      <Typography variant="h6">Historical trend</Typography>
                      <TextField
                        select
                        size="small"
                        value={trendParam}
                        onChange={(e) => setTrendParam(e.target.value)}
                      >
                        {state.measurements
                          .filter(
                            (m) =>
                              m.som_id === selected &&
                              typeof m.value === "number",
                          )
                          .map((m) => (
                            <MenuItem
                              key={m.parameter.id}
                              value={m.parameter.id}
                            >
                              {m.parameter.name}
                            </MenuItem>
                          ))}
                      </TextField>
                    </div>
                    <ResponsiveContainer width="100%" height={210}>
                      <AreaChart data={chart}>
                        <defs>
                          <linearGradient id="fill" x1="0" y1="0" x2="0" y2="1">
                            <stop
                              offset="0%"
                              stopColor="#65dfc1"
                              stopOpacity={0.3}
                            />
                            <stop
                              offset="100%"
                              stopColor="#65dfc1"
                              stopOpacity={0}
                            />
                          </linearGradient>
                        </defs>
                        <CartesianGrid stroke="#243347" vertical={false} />
                        <XAxis
                          dataKey="time"
                          minTickGap={70}
                          tick={{ fill: "#8a9bb0", fontSize: 11 }}
                        />
                        <YAxis tick={{ fill: "#8a9bb0", fontSize: 11 }} />
                        <Tooltip
                          contentStyle={{
                            background: "#111e2d",
                            border: "1px solid #243347",
                          }}
                        />
                        <Area
                          type="monotone"
                          dataKey="value"
                          stroke="#65dfc1"
                          fill="url(#fill)"
                          isAnimationActive={false}
                        />
                      </AreaChart>
                    </ResponsiveContainer>
                  </div>
                  <div className="toolbar">
                    <TextField
                      select
                      size="small"
                      label="BIT category"
                      value={category}
                      sx={{ minWidth: 180 }}
                      onChange={(e) => setCategory(e.target.value)}
                    >
                      {[
                        "All",
                        ...new Set(
                          state.measurements
                            .filter((m) => m.som_id === selected)
                            .map((m) => m.parameter.category),
                        ),
                      ].map((c) => (
                        <MenuItem key={c} value={c}>
                          {c}
                        </MenuItem>
                      ))}
                    </TextField>
                    <span className="muted">
                      {measurements.length} parameters ·{" "}
                      {selectedMember.quality?.state}
                    </span>
                  </div>
                  <div className="panel">
                    <TableContainer>
                      <Table>
                        <TableHead>
                          <TableRow>
                            {[
                              "Parameter",
                              "Value",
                              "Validity / severity",
                              "Collection / publish",
                              "Source timestamp",
                            ].map((x) => (
                              <TableCell key={x}>{x}</TableCell>
                            ))}
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {measurements
                            .sort((a, b) =>
                              a.parameter.id.localeCompare(b.parameter.id),
                            )
                            .map((m) => (
                              <TableRow
                                hover
                                key={m.parameter.id}
                                onClick={() => setTrendParam(m.parameter.id)}
                                sx={{ cursor: "pointer" }}
                              >
                                <TableCell>
                                  <strong>{m.parameter.name}</strong>
                                  <div className="mono muted">
                                    {m.parameter.id}
                                  </div>
                                </TableCell>
                                <TableCell className="mono">
                                  {format(m.value)} {m.parameter.unit}
                                </TableCell>
                                <TableCell>
                                  <Status
                                    value={
                                      m.validity === "VALID"
                                        ? m.severity
                                        : m.validity
                                    }
                                  />
                                </TableCell>
                                <TableCell>
                                  {m.parameter.collection_s}s /{" "}
                                  {m.parameter.publication_s}s
                                </TableCell>
                                <TableCell>{time(m.source_time)}</TableCell>
                              </TableRow>
                            ))}
                        </TableBody>
                      </Table>
                    </TableContainer>
                  </div>
                </>
              )}
              {page === "faults" && (
                <>
                  <div className="panel" style={{ marginBottom: 20 }}>
                    <Typography variant="h6">Current BIT violations</Typography>
                    <p className="muted">
                      Live thresholds; acknowledgment does not change the
                      measured condition. Unavailable or stale values retain
                      their last known fault.
                    </p>
                    {currentViolations.length ? (
                      currentViolations.slice(0, 20).map((m) => (
                        <div
                          className="event-line"
                          key={`${m.som_id}/${m.parameter.id}`}
                        >
                          <Status
                            value={
                              m.validity === "VALID" ? m.severity : m.validity
                            }
                          />
                          <Button
                            onClick={() => {
                              inspect(m.som_id);
                              setTrendParam(m.parameter.id);
                            }}
                          >
                            {m.som_id}
                          </Button>
                          <span>
                            {m.parameter.name}:{" "}
                            <strong>
                              {format(m.value)} {m.parameter.unit}
                            </strong>
                          </span>
                        </div>
                      ))
                    ) : (
                      <p className="muted">
                        No current threshold violations match this filter.
                      </p>
                    )}
                    {currentViolations.length > 20 && (
                      <p className="muted">
                        Showing 20 of {currentViolations.length}; narrow the
                        search to inspect more.
                      </p>
                    )}
                  </div>
                  <Typography variant="h6">Event archive</Typography>
                  <div className="toolbar">
                    <TextField
                      select
                      label="Severity"
                      value={severity}
                      size="small"
                      sx={{ minWidth: 160 }}
                      onChange={(e) => setSeverity(e.target.value)}
                    >
                      {["All", "CRITICAL", "WARNING", "INFO"].map((s) => (
                        <MenuItem key={s} value={s}>
                          {s}
                        </MenuItem>
                      ))}
                    </TextField>
                    <TextField
                      select
                      label="Event state"
                      size="small"
                      value={eventState}
                      sx={{ minWidth: 150 }}
                      onChange={(e) => setEventState(e.target.value)}
                    >
                      {["All", "ACTIVE", "CLEARED", "NOTICE"].map((value) => (
                        <MenuItem key={value} value={value}>
                          {value}
                        </MenuItem>
                      ))}
                    </TextField>
                    <TextField
                      select
                      label="Sort by time"
                      size="small"
                      value={eventOrder}
                      sx={{ minWidth: 150 }}
                      onChange={(e) => setEventOrder(e.target.value)}
                    >
                      {["Newest", "Oldest"].map((value) => (
                        <MenuItem key={value} value={value}>
                          {value}
                        </MenuItem>
                      ))}
                    </TextField>
                    <span className="muted">
                      Acknowledgment does not clear a fault
                    </span>
                  </div>
                  {gaps.map((g, i) => (
                    <Alert severity="warning" key={i} sx={{ mb: 1 }}>
                      Historical gap · {g.som_id}: {g.description}
                      {g.start ? ` (${g.start}–${g.end})` : ""}
                    </Alert>
                  ))}
                  <div className="panel">
                    <TableContainer>
                      <Table>
                        <TableHead>
                          <TableRow>
                            {[
                              "Severity",
                              "Source / category",
                              "Event",
                              "Time",
                              "State",
                              "Acknowledgment",
                            ].map((x) => (
                              <TableCell key={x}>{x}</TableCell>
                            ))}
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {events
                            .filter(
                              (e) =>
                                (severity === "All" ||
                                  e.severity === severity) &&
                                (eventState === "All" ||
                                  e.state === eventState) &&
                                `${e.som_id} ${e.description} ${e.category}`
                                  .toLowerCase()
                                  .includes(search.toLowerCase()),
                            )
                            .sort((a, b) =>
                              eventOrder === "Newest"
                                ? b.source_time - a.source_time
                                : a.source_time - b.source_time,
                            )
                            .map((e) => {
                              const acked = acks.some(
                                (a) =>
                                  a.som_id === e.som_id &&
                                  a.boot_id === e.boot_id &&
                                  a.sequence === e.sequence,
                              );
                              return (
                                <TableRow
                                  key={`${e.som_id}/${e.boot_id}/${e.sequence}`}
                                >
                                  <TableCell>
                                    <Status value={e.severity} />
                                  </TableCell>
                                  <TableCell>
                                    <Button
                                      onClick={() => {
                                        inspect(e.som_id);
                                        if (e.parameter_id)
                                          setTrendParam(e.parameter_id);
                                      }}
                                    >
                                      {e.som_id}
                                    </Button>
                                    <div className="muted">{e.category}</div>
                                  </TableCell>
                                  <TableCell>{e.description}</TableCell>
                                  <TableCell>{time(e.source_time)}</TableCell>
                                  <TableCell>
                                    {e.state === "ACTIVE" ? "RAISED" : e.state}
                                  </TableCell>
                                  <TableCell>
                                    {acked ? (
                                      <Status value="SUCCEEDED" />
                                    ) : (
                                      <Button
                                        disabled={operator.role === "viewer"}
                                        onClick={() =>
                                          void api(
                                            `events/${e.som_id}/${e.boot_id}/${e.sequence}/ack`,
                                            {},
                                          )
                                            .then(refreshArchive)
                                            .catch((e) => setError(String(e)))
                                        }
                                      >
                                        Acknowledge
                                      </Button>
                                    )}
                                  </TableCell>
                                </TableRow>
                              );
                            })}
                        </TableBody>
                      </Table>
                    </TableContainer>
                    {!events.length && (
                      <p className="empty">No archived events.</p>
                    )}
                  </div>
                </>
              )}
              {(page === "operations" || page === "config") && (
                <>
                  <div className="toolbar">
                    <TextField
                      select
                      size="small"
                      label="Target SOM"
                      sx={{ minWidth: 230 }}
                      value={selected}
                      onChange={(e) => setSelected(e.target.value)}
                    >
                      {state.members.map((n) => (
                        <MenuItem key={n.som_id} value={n.som_id}>
                          {n.som_id} · {n.lifecycle}
                        </MenuItem>
                      ))}
                    </TextField>
                    {selectedMember && (
                      <>
                        <Status value={selectedMember.lifecycle} />
                        <Chip
                          size="small"
                          label={`Boot ${selectedMember.boot_id.slice(0, 8)}`}
                        />
                      </>
                    )}
                  </div>
                  {page === "operations" ? (
                    <>
                      <div className="panel">
                        <Typography variant="h6">Command center</Typography>
                        <p className="muted">
                          Disruptive commands require admin permission and
                          majority authority. Transfer an active master before
                          maintenance.
                        </p>
                        <div className="command-buttons">
                          {[
                            "start",
                            "stop",
                            "restart",
                            "reset",
                            "approve",
                            "status",
                          ].map((kind) => (
                            <Button
                              key={kind}
                              variant={
                                kind === "start" ? "contained" : "outlined"
                              }
                              disabled={
                                !selected || busy || operator.role === "viewer"
                              }
                              onClick={() => request(kind)}
                            >
                              {kind === "approve"
                                ? "Approve recovery"
                                : kind[0].toUpperCase() + kind.slice(1)}
                            </Button>
                          ))}
                        </div>
                        <div className="toolbar">
                          <TextField
                            size="small"
                            label="Artifact identity"
                            placeholder="sim-pl-2.0"
                            value={artifact}
                            onChange={(e) => setArtifact(e.target.value)}
                          />
                          {["program_pl", "update_software"].map((k) => (
                            <Button
                              disabled={
                                !selected ||
                                !artifact ||
                                busy ||
                                operator.role !== "admin"
                              }
                              key={k}
                              onClick={() => request(k)}
                            >
                              {k === "program_pl"
                                ? "Program PL"
                                : "Update software"}
                            </Button>
                          ))}
                        </div>
                        <div className="toolbar">
                          <TextField
                            select
                            size="small"
                            label="Successor"
                            sx={{ minWidth: 220 }}
                            value={successor}
                            onChange={(e) => setSuccessor(e.target.value)}
                          >
                            {state.members
                              .filter(
                                (n) =>
                                  n.subsystem === selectedMember?.subsystem &&
                                  n.som_id !== selected &&
                                  ["READY", "ACTIVE"].includes(n.lifecycle),
                              )
                              .map((n) => (
                                <MenuItem key={n.som_id} value={n.som_id}>
                                  {n.som_id}
                                </MenuItem>
                              ))}
                          </TextField>
                          <Button
                            disabled={
                              !selected ||
                              !successor ||
                              busy ||
                              operator.role !== "admin"
                            }
                            onClick={() => request("transfer")}
                          >
                            Transfer master
                          </Button>
                        </div>
                      </div>
                      {simulation && (
                        <div className="panel" style={{ marginTop: 20 }}>
                          <Typography variant="h6">
                            Simulation controls
                          </Typography>
                          <p className="muted">
                            Faults traverse real agent and archive interfaces.
                          </p>
                          <div className="command-buttons">
                            {[
                              ["Temperature 92°C", "temperature", 92],
                              ["Master failure", "master_failure", undefined],
                              [
                                "Reconnect / quarantine",
                                "reconnect",
                                undefined,
                              ],
                              ["Journal overflow", "overflow", 100],
                              ["Degrade PTP", "ptp", "DEGRADED"],
                            ].map(([label, action, value]) => (
                              <Button
                                key={String(action)}
                                disabled={
                                  !selected || operator.role !== "admin"
                                }
                                onClick={() =>
                                  void inject(
                                    String(action),
                                    value as string | number | undefined,
                                  )
                                }
                                variant="outlined"
                              >
                                {label}
                              </Button>
                            ))}
                          </div>
                        </div>
                      )}
                      <div className="panel" style={{ marginTop: 20 }}>
                        <Typography variant="h6">Execution history</Typography>
                        <TableContainer>
                          <Table>
                            <TableHead>
                              <TableRow>
                                {[
                                  "Command / target",
                                  "Operator",
                                  "State",
                                  "Requested",
                                  "Result",
                                ].map((x) => (
                                  <TableCell key={x}>{x}</TableCell>
                                ))}
                              </TableRow>
                            </TableHead>
                            <TableBody>
                              {operations.map((c) => (
                                <TableRow key={c.id}>
                                  <TableCell>
                                    <strong>{c.kind}</strong>
                                    <div className="muted">
                                      {c.target} · {c.id.slice(0, 8)}
                                    </div>
                                  </TableCell>
                                  <TableCell>{c.operator}</TableCell>
                                  <TableCell>
                                    <Status value={c.state} />
                                  </TableCell>
                                  <TableCell>{time(c.requested_at)}</TableCell>
                                  <TableCell>
                                    {c.error ??
                                      (c.completed_at
                                        ? "Reconciled / completed"
                                        : "Pending reconciliation")}
                                  </TableCell>
                                </TableRow>
                              ))}
                            </TableBody>
                          </Table>
                        </TableContainer>
                      </div>
                    </>
                  ) : (
                    <>
                      <div className="panel">
                        <Typography variant="h6">Monitoring policy</Typography>
                        <p className="muted">
                          Edit parameter definitions, rates, thresholds,
                          deadbands and retention in seconds. Changes are
                          validated, fenced and archived. Voting inventory and
                          endpoints require a coordinated restart using the
                          inventory file. Operator identities are configured
                          securely on the CPU.
                        </p>
                        <TextField
                          multiline
                          minRows={16}
                          fullWidth
                          label="Parameter policy JSON"
                          value={config}
                          onChange={(e) => setConfig(e.target.value)}
                          slotProps={{
                            input: {
                              style: { fontFamily: "monospace", fontSize: 12 },
                            },
                          }}
                        />
                        <Button
                          sx={{ mt: 2 }}
                          variant="contained"
                          disabled={
                            !selected || busy || operator.role !== "admin"
                          }
                          onClick={() => request("configure")}
                        >
                          Validate and apply policy
                        </Button>
                      </div>
                      <div className="panel" style={{ marginTop: 20 }}>
                        <Typography variant="h6">
                          Configured inventory
                        </Typography>
                        {nodeTable(members)}
                      </div>
                    </>
                  )}
                </>
              )}
              {notice && (
                <Alert
                  sx={{ mt: 2 }}
                  severity="info"
                  onClose={() => setNotice("")}
                >
                  {notice}
                </Alert>
              )}
              <footer className="main-footer">
                <span>
                  BITMesh / v0.1 ·{" "}
                  {simulation ? "Simulated hardware" : "Hardware adapter mode"}
                </span>
                <span>
                  CPU p95{" "}
                  {state.metrics.source_to_cpu_p95_ms?.toFixed(0) ?? "—"} ms ·
                  Browser drops {state.metrics.browser_drops} · Archive drops{" "}
                  {state.metrics.archive_drops}
                </span>
              </footer>
            </>
          )}
        </main>
      </div>
      <Dialog open={!!confirm} onClose={() => !busy && setConfirm("")}>
        <DialogTitle>Confirm {confirm.replaceAll("_", " ")}</DialogTitle>
        <DialogContent>
          <p>
            Target: <strong>{selected}</strong>
          </p>
          <p>
            Boot session:{" "}
            <span className="mono">{selectedMember?.boot_id}</span>
          </p>
          <p>
            {confirm === "approve"
              ? "Approval restores processing and master eligibility for this boot session."
              : confirm === "program_pl"
                ? "PL programming may interrupt all Ethernet communication. Completion requires post-operation reconciliation."
                : "This action is audited with your operator identity and may change system availability."}
          </p>
        </DialogContent>
        <DialogActions>
          <Button disabled={busy} onClick={() => setConfirm("")}>
            Cancel
          </Button>
          <Button
            disabled={busy}
            variant="contained"
            onClick={() => void perform(confirm)}
          >
            {busy ? "Executing…" : "Confirm action"}
          </Button>
        </DialogActions>
      </Dialog>
    </div>
  );
}
createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <App />
    </ThemeProvider>
  </React.StrictMode>,
);
