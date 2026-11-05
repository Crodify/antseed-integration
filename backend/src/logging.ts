type LogValue = string | number | boolean | null | undefined;
type LogData = Record<string, LogValue | LogValue[] | Record<string, unknown>>;

type LogLevel = "info" | "warn" | "error";

function base(event: string, data?: LogData): Record<string, unknown> {
  return {
    ts: new Date().toISOString(),
    event,
    ...(data ?? {})
  };
}

function emit(level: LogLevel, event: string, data?: LogData): void {
  const payload = base(event, data);
  if (level === "error") {
    console.error(event, payload);
    return;
  }
  if (level === "warn") {
    console.warn(event, payload);
    return;
  }

  console.log(event, payload);
}

export function logInfo(event: string, data?: LogData): void {
  emit("info", event, data);
}

export function logWarn(event: string, data?: LogData): void {
  emit("warn", event, data);
}

export function logError(event: string, data?: LogData): void {
  emit("error", event, data);
}

export function redactAddress(value: string | undefined): string | undefined {
  if (!value) return undefined;
  const normalized = value.toLowerCase();
  if (!normalized.startsWith("0x") || normalized.length < 10) return "redacted";
  return `${normalized.slice(0, 6)}...${normalized.slice(-4)}`;
}

export function redactHash(value: string | undefined): string | undefined {
  if (!value) return undefined;
  const normalized = value.toLowerCase();
  if (!normalized.startsWith("0x") || normalized.length < 14) return "redacted";
  return `${normalized.slice(0, 10)}...${normalized.slice(-4)}`;
}

export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "unknown error";
}
