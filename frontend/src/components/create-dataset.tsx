import { python } from "@codemirror/lang-python";
import { zodResolver } from "@hookform/resolvers/zod";
import { useMutation } from "@tanstack/react-query";
import CodeMirror from "@uiw/react-codemirror";
import { ArrowLeftIcon, ExternalLinkIcon, PlusIcon, XIcon } from "lucide-react";
import { useFieldArray, useForm, useWatch } from "react-hook-form";
import { z } from "zod";

import type { DatasetProposalPayload, ProposalResponse } from "@/lib/api";

import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Form,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { useDatasets } from "@/hooks/use-datasets";
import { proposeDataset } from "@/lib/api";

const SCHEMA_TYPES = ["str", "int", "float", "bool"] as const;
const CALENDARS = ["everyday", "weekday", "always-open", "nyse-daily"] as const;
const GRANULARITIES = ["1s", "1m", "1h", "1d"] as const;

const DEFAULT_BUILDER = `from datetime import datetime
from typing import Any


def build(
    dependencies: dict[str, dict[datetime, list[dict]]],
    timestamp: datetime,
) -> list[dict[str, Any]]:
    # return one dict per row for this timestamp, matching the schema
    return []
`;

const formSchema = z.object({
  name: z
    .string()
    .regex(
      /^[a-z0-9][a-z0-9_-]*$/,
      "lowercase alphanumeric with - or _ (becomes the directory name)",
    ),
  version: z.string().regex(/^\d+\.\d+\.\d+$/, "semver like 0.1.0"),
  calendar: z.enum(CALENDARS),
  granularity: z.enum(GRANULARITIES),
  startDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, "YYYY-MM-DD"),
  schemaFields: z
    .array(
      z.object({
        key: z.string().min(1, "field name required"),
        type: z.enum(SCHEMA_TYPES),
      }),
    )
    .min(1, "at least one schema field"),
  dependencies: z.array(
    z.object({
      dataset: z.string().min(1, "pick a dataset"),
      lookback: z
        .string()
        .regex(/^\d+[dhms]$/, "like 5d, 24h, 30m, 60s")
        .or(z.literal("")),
    }),
  ),
  builderScript: z
    .string()
    .refine(
      (script) => /def\s+build\s*\(/.test(script),
      "must define a build() function",
    ),
  authorName: z.string().trim().min(1, "your name is required"),
  team: z.string().trim().min(1, "team is required"),
  discordUser: z.string().trim().min(1, "discord username is required"),
  description: z
    .string()
    .trim()
    .min(10, "a sentence or two about what this dataset is for"),
  envVars: z.boolean(),
  requirementsTxt: z.string(),
  envTemplate: z.string(),
});

type FormValues = z.infer<typeof formSchema>;

function toPayload(values: FormValues): DatasetProposalPayload {
  return {
    name: values.name,
    version: values.version,
    calendar: values.calendar,
    granularity: values.granularity,
    start_date: values.startDate,
    schema: Object.fromEntries(
      values.schemaFields.map((field) => [field.key, field.type]),
    ),
    dependencies: values.dependencies.map((dep) => {
      const [name, version] = dep.dataset.split("@");
      return { name, version, lookback: dep.lookback || undefined };
    }),
    builder_script: values.builderScript,
    author_name: values.authorName,
    team: values.team,
    discord_user: values.discordUser,
    description: values.description,
    env_vars: values.envVars,
    requirements_txt: values.requirementsTxt.trim()
      ? values.requirementsTxt
      : undefined,
    env_template: values.envTemplate.trim() ? values.envTemplate : undefined,
  };
}

function SectionHeading({ children }: { children: React.ReactNode }) {
  return <h3 className="border-b pb-2 text-base font-medium">{children}</h3>;
}

export function CreateDataset({ onBack }: { onBack: () => void }) {
  const { data: datasets } = useDatasets();

  const form = useForm<FormValues>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      name: "",
      version: "0.1.0",
      calendar: "everyday",
      granularity: "1d",
      startDate: "",
      authorName: "",
      team: "",
      discordUser: "",
      description: "",
      schemaFields: [{ key: "", type: "str" }],
      dependencies: [],
      builderScript: DEFAULT_BUILDER,
      envVars: false,
      requirementsTxt: "",
      envTemplate: "",
    },
  });

  const schemaRows = useFieldArray({
    control: form.control,
    name: "schemaFields",
  });
  const depRows = useFieldArray({
    control: form.control,
    name: "dependencies",
  });

  const envVarsEnabled = useWatch({
    control: form.control,
    name: "envVars",
  });

  const mutation = useMutation<ProposalResponse, Error, FormValues>({
    mutationFn: (values) => proposeDataset(toPayload(values)),
  });

  if (mutation.isSuccess) {
    return (
      <div className="space-y-4">
        <h2 className="text-xl font-semibold">Proposal opened</h2>
        <p className="text-muted-foreground text-sm">
          <code className="font-mono">
            {mutation.data.dataset_name}/{mutation.data.dataset_version}
          </code>{" "}
          was submitted as a pull request. It goes live after review, merge, and
          the next server restart.
        </p>
        <p className="text-sm">
          <a
            href={mutation.data.pr_url}
            target="_blank"
            rel="noreferrer"
            className="text-primary inline-flex items-center gap-1 underline underline-offset-4"
          >
            {mutation.data.pr_url} <ExternalLinkIcon className="size-3.5" />
          </a>
        </p>
        {form.getValues("envVars") && (
          <p className="border-destructive/50 text-muted-foreground rounded-md border p-3 text-sm">
            this dataset needs env vars: after merge, someone must place the
            real <code className="font-mono">.env</code> on the server (see the
            PR checklist) before it can build.
          </p>
        )}
        <div className="flex gap-3">
          <Button variant="outline" size="sm" onClick={onBack}>
            back to datasets
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => {
              mutation.reset();
              form.reset();
            }}
          >
            create another
          </Button>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <Button
        variant="ghost"
        size="sm"
        onClick={onBack}
        className="text-muted-foreground -ml-2"
      >
        <ArrowLeftIcon /> back
      </Button>
      <h2 className="text-xl font-semibold">New dataset</h2>
      <p className="text-muted-foreground text-sm">
        Submitting opens a pull request with the dataset files — nothing runs
        until a reviewer merges it.
      </p>

      <Form {...form}>
        <form
          onSubmit={form.handleSubmit((values) => mutation.mutate(values))}
          className="space-y-8"
        >
          <section className="space-y-4">
            <SectionHeading>Proposer</SectionHeading>
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
              <FormField
                control={form.control}
                name="authorName"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Your name</FormLabel>
                    <FormControl>
                      <Input {...field} placeholder="Jane Doe" />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
              <FormField
                control={form.control}
                name="team"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Wat Street team</FormLabel>
                    <FormControl>
                      <Input {...field} placeholder="quant" />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
              <FormField
                control={form.control}
                name="discordUser"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Discord user</FormLabel>
                    <FormControl>
                      <Input
                        {...field}
                        placeholder="janedoe"
                        className="font-mono"
                      />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
            </div>
            <FormField
              control={form.control}
              name="description"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>What is this dataset for?</FormLabel>
                  <FormControl>
                    <Textarea
                      {...field}
                      rows={2}
                      placeholder="brief explanation of the data and what it will be used for"
                    />
                  </FormControl>
                  <FormDescription>
                    shown to reviewers in the pull request
                  </FormDescription>
                  <FormMessage />
                </FormItem>
              )}
            />
          </section>

          <section className="space-y-4">
            <SectionHeading>Basics</SectionHeading>
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
              <FormField
                control={form.control}
                name="name"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Name</FormLabel>
                    <FormControl>
                      <Input
                        {...field}
                        placeholder="my-dataset"
                        className="font-mono"
                      />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
              <FormField
                control={form.control}
                name="version"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Version</FormLabel>
                    <FormControl>
                      <Input {...field} className="font-mono" />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
              <FormField
                control={form.control}
                name="calendar"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Calendar</FormLabel>
                    <Select value={field.value} onValueChange={field.onChange}>
                      <FormControl>
                        <SelectTrigger className="w-full">
                          <SelectValue />
                        </SelectTrigger>
                      </FormControl>
                      <SelectContent>
                        {CALENDARS.map((calendar) => (
                          <SelectItem key={calendar} value={calendar}>
                            {calendar}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <FormMessage />
                  </FormItem>
                )}
              />
              <FormField
                control={form.control}
                name="granularity"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Granularity</FormLabel>
                    <Select value={field.value} onValueChange={field.onChange}>
                      <FormControl>
                        <SelectTrigger className="w-full">
                          <SelectValue />
                        </SelectTrigger>
                      </FormControl>
                      <SelectContent>
                        {GRANULARITIES.map((granularity) => (
                          <SelectItem key={granularity} value={granularity}>
                            {granularity}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <FormMessage />
                  </FormItem>
                )}
              />
              <FormField
                control={form.control}
                name="startDate"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Start date</FormLabel>
                    <FormControl>
                      <Input
                        {...field}
                        placeholder="2024-01-01"
                        className="font-mono"
                      />
                    </FormControl>
                    <FormDescription>
                      earliest date data can be built for
                    </FormDescription>
                    <FormMessage />
                  </FormItem>
                )}
              />
            </div>
          </section>

          <section className="space-y-4">
            <SectionHeading>Schema</SectionHeading>
            {schemaRows.fields.map((row, index) => (
              <div key={row.id} className="flex items-start gap-3">
                <FormField
                  control={form.control}
                  name={`schemaFields.${index}.key`}
                  render={({ field }) => (
                    <FormItem className="flex-1">
                      <FormControl>
                        <Input
                          {...field}
                          placeholder="field name"
                          className="font-mono"
                        />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <FormField
                  control={form.control}
                  name={`schemaFields.${index}.type`}
                  render={({ field }) => (
                    <FormItem className="w-32">
                      <Select
                        value={field.value}
                        onValueChange={field.onChange}
                      >
                        <FormControl>
                          <SelectTrigger className="w-full">
                            <SelectValue />
                          </SelectTrigger>
                        </FormControl>
                        <SelectContent>
                          {SCHEMA_TYPES.map((type) => (
                            <SelectItem key={type} value={type}>
                              {type}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  disabled={schemaRows.fields.length === 1}
                  onClick={() => schemaRows.remove(index)}
                >
                  <XIcon />
                </Button>
              </div>
            ))}
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => schemaRows.append({ key: "", type: "str" })}
            >
              <PlusIcon /> add field
            </Button>
          </section>

          <section className="space-y-4">
            <SectionHeading>Dependencies</SectionHeading>
            {depRows.fields.length === 0 && (
              <p className="text-muted-foreground text-sm">
                none — this is a root dataset
              </p>
            )}
            {depRows.fields.map((row, index) => (
              <div key={row.id} className="flex items-start gap-3">
                <FormField
                  control={form.control}
                  name={`dependencies.${index}.dataset`}
                  render={({ field }) => (
                    <FormItem className="flex-1">
                      <Select
                        value={field.value}
                        onValueChange={field.onChange}
                      >
                        <FormControl>
                          <SelectTrigger className="w-full font-mono">
                            <SelectValue placeholder="pick a dataset" />
                          </SelectTrigger>
                        </FormControl>
                        <SelectContent>
                          {(datasets ?? []).map((dataset) => (
                            <SelectItem
                              key={`${dataset.name}@${dataset.version}`}
                              value={`${dataset.name}@${dataset.version}`}
                              className="font-mono"
                            >
                              {dataset.name}@{dataset.version}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <FormField
                  control={form.control}
                  name={`dependencies.${index}.lookback`}
                  render={({ field }) => (
                    <FormItem className="w-40">
                      <FormControl>
                        <Input
                          {...field}
                          placeholder="lookback (5d)"
                          className="font-mono"
                        />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  onClick={() => depRows.remove(index)}
                >
                  <XIcon />
                </Button>
              </div>
            ))}
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => depRows.append({ dataset: "", lookback: "" })}
            >
              <PlusIcon /> add dependency
            </Button>
          </section>

          <section className="space-y-4">
            <SectionHeading>Builder script</SectionHeading>
            <FormField
              control={form.control}
              name="builderScript"
              render={({ field }) => (
                <FormItem>
                  <FormControl>
                    <div className="overflow-hidden rounded-md border text-sm">
                      <CodeMirror
                        value={field.value}
                        onChange={field.onChange}
                        theme="dark"
                        height="320px"
                        extensions={[python()]}
                      />
                    </div>
                  </FormControl>
                  <FormDescription>
                    build(dependencies, timestamp) returning one dict per row —
                    this code runs on the server after review
                  </FormDescription>
                  <FormMessage />
                </FormItem>
              )}
            />
          </section>

          <section className="space-y-4">
            <SectionHeading>Extras</SectionHeading>
            <FormField
              control={form.control}
              name="requirementsTxt"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>requirements.txt (optional)</FormLabel>
                  <FormControl>
                    <Textarea
                      {...field}
                      rows={3}
                      placeholder={"pandas>=2.0\nrequests"}
                      className="font-mono"
                    />
                  </FormControl>
                  <FormDescription>
                    python packages installed into the builder&apos;s own venv
                  </FormDescription>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name="envVars"
              render={({ field }) => (
                <FormItem className="flex items-center gap-2">
                  <FormControl>
                    <Checkbox
                      checked={field.value}
                      onCheckedChange={(checked) =>
                        field.onChange(checked === true)
                      }
                    />
                  </FormControl>
                  <FormLabel className="font-normal">
                    builder needs environment variables (API keys, secrets)
                  </FormLabel>
                </FormItem>
              )}
            />
            {envVarsEnabled && (
              <FormField
                control={form.control}
                name="envTemplate"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>.env.template</FormLabel>
                    <FormControl>
                      <Textarea
                        {...field}
                        rows={3}
                        placeholder={"API_KEY=\nAPI_SECRET="}
                        className="font-mono"
                      />
                    </FormControl>
                    <FormDescription>
                      documents the required variables — the real .env never
                      goes in the PR and must be placed on the server manually
                    </FormDescription>
                    <FormMessage />
                  </FormItem>
                )}
              />
            )}
          </section>

          {mutation.isError && (
            <div className="border-destructive/50 rounded-md border p-4">
              <p className="text-destructive text-sm">
                {mutation.error.message}
              </p>
            </div>
          )}

          <div className="flex items-center gap-3">
            <Button type="submit" disabled={mutation.isPending}>
              {mutation.isPending ? "opening PR..." : "open PR"}
            </Button>
            <span className="text-muted-foreground text-sm">
              opens a pull request for review — nothing deploys directly
            </span>
          </div>
        </form>
      </Form>
    </div>
  );
}
