import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import type { StructuredType } from "@/lib/schemaEditor";
import { cn } from "@/lib/utils";
import { typeLabel, typeOptionsForDepth } from "./schemaTypes";

export function SchemaTypeSelect({
  value,
  depth,
  readOnly = false,
  onChange,
}: {
  value: StructuredType;
  depth: number;
  readOnly?: boolean;
  onChange: (value: StructuredType) => void;
}) {
  return (
    <Select value={value} disabled={readOnly} onValueChange={(next) => onChange(next as StructuredType)}>
      <SelectTrigger className={cn("h-8 w-full min-w-0", readOnly && "opacity-100 disabled:opacity-100")}>
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        {typeOptionsForDepth(depth, value).map((type) => (
          <SelectItem key={type} value={type}>
            {typeLabel(type)}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}
