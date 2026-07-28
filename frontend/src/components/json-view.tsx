import { Fragment, type ReactNode } from "react";

const INDENT = "  ";

/**
 * recursive syntax-highlighted json renderer. produces the same pretty-printed
 * layout as JSON.stringify(value, null, 2) but as jsx spans, so no manual
 * escaping or innerHTML is needed.
 */
export function JsonView({ value }: { value: unknown }) {
  return <>{renderValue(value, 0)}</>;
}

function renderValue(value: unknown, depth: number): ReactNode {
  if (value === null || value === undefined) {
    return <span className="json-null">null</span>;
  }
  if (typeof value === "boolean") {
    return <span className="json-boolean">{String(value)}</span>;
  }
  if (typeof value === "number") {
    return <span className="json-number">{String(value)}</span>;
  }
  if (typeof value === "string") {
    return <span className="json-string">{JSON.stringify(value)}</span>;
  }
  if (Array.isArray(value)) {
    if (value.length === 0) {
      return "[]";
    }
    const inner = INDENT.repeat(depth + 1);
    return (
      <>
        {"[\n"}
        {value.map((item, i) => (
          <Fragment key={i}>
            {inner}
            {renderValue(item, depth + 1)}
            {i < value.length - 1 ? "," : ""}
            {"\n"}
          </Fragment>
        ))}
        {INDENT.repeat(depth)}
        {"]"}
      </>
    );
  }
  const entries = Object.entries(value as Record<string, unknown>);
  if (entries.length === 0) {
    return "{}";
  }
  const inner = INDENT.repeat(depth + 1);
  return (
    <>
      {"{\n"}
      {entries.map(([key, val], i) => (
        <Fragment key={key}>
          {inner}
          <span className="json-key">{JSON.stringify(key)}</span>
          {": "}
          {renderValue(val, depth + 1)}
          {i < entries.length - 1 ? "," : ""}
          {"\n"}
        </Fragment>
      ))}
      {INDENT.repeat(depth)}
      {"}"}
    </>
  );
}
