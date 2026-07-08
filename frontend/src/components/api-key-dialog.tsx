import { zodResolver } from "@hookform/resolvers/zod";
import { useQueryClient } from "@tanstack/react-query";
import { useForm } from "react-hook-form";
import { z } from "zod";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Form,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import { useApiKey } from "@/hooks/use-api-key";
import { maskApiKey } from "@/lib/api-key";

const formSchema = z.object({
  apiKey: z
    .string()
    .trim()
    .regex(/^dsk_/, "keys start with dsk_")
    .min(8, "key looks too short"),
});

type FormValues = z.infer<typeof formSchema>;

export function ApiKeyDialog() {
  const { apiKey, saveApiKey, clearApiKey, dialogOpen, setDialogOpen } =
    useApiKey();
  const queryClient = useQueryClient();

  const form = useForm<FormValues>({
    resolver: zodResolver(formSchema),
    defaultValues: { apiKey: "" },
  });

  function onSubmit(values: FormValues) {
    saveApiKey(values.apiKey.trim());
    form.reset();
    setDialogOpen(false);
    // refetch everything that failed without a key
    void queryClient.invalidateQueries();
  }

  return (
    <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>API key</DialogTitle>
          <DialogDescription>
            Requests are authenticated with a bearer key. Paste your{" "}
            <code className="font-mono">dsk_</code> key — it is stored only in
            this browser.
          </DialogDescription>
        </DialogHeader>
        {apiKey && (
          <div className="text-muted-foreground flex items-center justify-between rounded-md border px-3 py-2 text-sm">
            <span className="font-mono">{maskApiKey(apiKey)}</span>
            <Button
              type="button"
              variant="ghost"
              size="sm"
              onClick={() => clearApiKey()}
            >
              clear
            </Button>
          </div>
        )}
        <Form {...form}>
          <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-4">
            <FormField
              control={form.control}
              name="apiKey"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>{apiKey ? "Replace key" : "Key"}</FormLabel>
                  <FormControl>
                    <Input
                      {...field}
                      type="password"
                      placeholder="dsk_..."
                      autoComplete="off"
                      className="font-mono"
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <div className="flex justify-end">
              <Button type="submit">Save</Button>
            </div>
          </form>
        </Form>
      </DialogContent>
    </Dialog>
  );
}
