/**
 * Labels tools - create labels in batch.
 */

import { z } from 'zod';
import { toolsMetadata } from '../../../config/metadata.js';
import { config } from '../../../config/env.js';
import { CreateLabelsOutputSchema } from '../../../schemas/outputs.js';
import { getLinearClient } from '../../../services/linear/client.js';
import { makeConcurrencyGate, withRetry, delay } from '../../../utils/limits.js';
import { logger } from '../../../utils/logger.js';
import { summarizeBatch } from '../../../utils/messages.js';
import { defineTool, type ToolContext, type ToolResult } from '../types.js';

// Create Labels
const CreateLabelsInputSchema = z.object({
  items: z
    .array(
      z.object({
        name: z.string().describe('Label name. Required.'),
        teamId: z
          .string()
          .optional()
          .describe(
            'Team UUID. If provided, label is team-scoped. If omitted, label is workspace-level. Use workspace_metadata to find team IDs.',
          ),
        color: z
          .string()
          .optional()
          .describe('Hex color code (e.g., "#10B981"). If omitted, Linear assigns a default color.'),
        description: z.string().optional().describe('Label description.'),
      }),
    )
    .min(1)
    .max(50)
    .describe('Labels to create.'),
  parallel: z.boolean().optional().describe('Run in parallel. Default: sequential.'),
});

export const createLabelsTool = defineTool({
  name: toolsMetadata.create_labels.name,
  title: toolsMetadata.create_labels.title,
  description: toolsMetadata.create_labels.description,
  inputSchema: CreateLabelsInputSchema,
  annotations: {
    readOnlyHint: false,
    destructiveHint: false,
  },

  handler: async (args, context: ToolContext): Promise<ToolResult> => {
    const client = await getLinearClient(context);
    const gate = makeConcurrencyGate(config.CONCURRENCY_LIMIT);

    const results: {
      index: number;
      ok: boolean;
      id?: string;
      error?: string;
      code?: string;
    }[] = [];

    for (let i = 0; i < args.items.length; i++) {
      const it = args.items[i];
      try {
        if (context.signal?.aborted) {
          throw new Error('Operation aborted');
        }

        // Add small delay between requests to avoid rate limits
        if (i > 0) {
          await delay(100);
        }

        const input: Record<string, unknown> = { name: it.name };
        if (it.teamId) input.teamId = it.teamId;
        if (it.color) input.color = it.color;
        if (it.description) input.description = it.description;

        const call = () => client.createIssueLabel(input as Parameters<typeof client.createIssueLabel>[0]);

        const payload = await withRetry(
          () => (args.items.length > 1 ? gate(call) : call()),
          { maxRetries: 3, baseDelayMs: 500 },
        );

        results.push({
          input: { name: it.name, teamId: it.teamId, color: it.color },
          success: payload.success ?? true,
          id: (payload.issueLabel as { id?: string } | null | undefined)?.id,
          // Legacy
          index: i,
          ok: payload.success ?? true,
        });
      } catch (error) {
        await logger.error('create_labels', {
          message: 'Failed to create label',
          index: i,
          error: (error as Error).message,
        });
        results.push({
          input: { name: it.name, teamId: it.teamId },
          success: false,
          error: {
            code: 'LINEAR_CREATE_ERROR',
            message: (error as Error).message,
            suggestions: ['Verify teamId with workspace_metadata.', 'Check if label name already exists.'],
          },
          // Legacy
          index: i,
          ok: false,
        });
      }
    }

    const succeeded = results.filter((r) => r.success).length;
    const failed = results.filter((r) => !r.success).length;

    const summary = {
      total: args.items.length,
      succeeded,
      failed,
      ok: succeeded,
    };

    const meta = {
      nextSteps: [
        'Use workspace_metadata to verify labels appear.',
        'Use create_issues or update_issues with labelNames to assign labels to issues.',
      ],
      relatedTools: ['workspace_metadata', 'create_issues', 'update_issues'],
    };

    const structured = CreateLabelsOutputSchema.parse({ results, summary, meta });

    const okIds = results
      .filter((r) => r.ok)
      .map((r) => r.id ?? `item[${String(r.index)}]`) as string[];

    const failures = results
      .filter((r) => !r.ok)
      .map((r) => {
        const err = r.error;
        if (typeof err === 'object' && err !== null) {
          const errObj = err as { message?: string; code?: string };
          return { index: r.index, id: undefined, error: errObj.message ?? String(err), code: errObj.code };
        }
        return { index: r.index, id: undefined, error: String(err ?? ''), code: undefined };
      });

    const text = summarizeBatch({
      action: 'Created labels',
      ok: summary.ok,
      total: args.items.length,
      okIdentifiers: okIds,
      failures,
      nextSteps: ['Use workspace_metadata to verify; use labelNames in create_issues/update_issues.'],
    });

    const parts: Array<{ type: 'text'; text: string }> = [{ type: 'text', text }];

    if (config.LINEAR_MCP_INCLUDE_JSON_IN_CONTENT) {
      parts.push({ type: 'text', text: JSON.stringify(structured) });
    }

    return { content: parts, structuredContent: structured };
  },
});
