import type {
  DocumentByName,
  GenericDataModel,
  TableNamesInDataModel,
  WithoutSystemFields,
} from "convex/server";
import type { CommitTsPlaceholder } from "convex/values";

/** Minimal logging surface for warnings; `globalThis.console` satisfies it. */
export type QueueLogger = { error: (...args: unknown[]) => void };

/**
 * The ordering fields every queue-lane document carries — added by
 * `queueTable`. `segment` reads back as a plain int64 (`bigint`); the
 * placeholder type only appears at write time.
 */
export type QueueItem = {
  segment: bigint | CommitTsPlaceholder;
  runAt?: number;
};

/** The tables in a data model that are queue lanes (built with `queueTable`). */
export type QueueTables<DataModel extends GenericDataModel> = {
  [Table in TableNamesInDataModel<DataModel>]: DocumentByName<
    DataModel,
    Table
  > extends QueueItem
    ? Table
    : never;
}[TableNamesInDataModel<DataModel>];

/** The consumer-defined fields of a lane's documents. */
export type QueuePayload<
  DataModel extends GenericDataModel,
  Table extends QueueTables<DataModel>,
> = Omit<
  WithoutSystemFields<DocumentByName<DataModel, Table>>,
  "segment" | "runAt"
>;
