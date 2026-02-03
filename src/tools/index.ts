import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import type { ZodObject, ZodRawShape, ZodTypeAny } from 'zod';
import { contextRegistry, getCurrentAuthContext } from '../core/context.js';
import { sharedTools, type ToolContext } from '../shared/tools/registry.js';
import { logger } from '../utils/logger.js';

/**
 * Extract the shape from a Zod schema, handling ZodEffects (refined schemas).
 * ZodEffects wraps the inner schema when using .refine(), .transform(), etc.
 */
function getSchemaShape(schema: ZodTypeAny): ZodRawShape | undefined {
  // If it's a ZodObject, return its shape directly
  if ('shape' in schema && typeof schema.shape === 'object') {
    return (schema as ZodObject<ZodRawShape>).shape;
  }

  // If it's a ZodEffects (from .refine(), .transform(), etc.), unwrap to get inner schema
  if ('_def' in schema && schema._def && typeof schema._def === 'object') {
    const def = schema._def as { schema?: ZodTypeAny; innerType?: ZodTypeAny };
    // ZodEffects stores the inner schema in _def.schema
    if (def.schema) {
      return getSchemaShape(def.schema);
    }
    // Some Zod versions use _def.innerType
    if (def.innerType) {
      return getSchemaShape(def.innerType);
    }
  }

  return undefined;
}

/**
 * Register all tools with the MCP server.
 * Uses shared tools that work in both Node and Workers.
 */
export function registerTools(server: McpServer): void {
  const registeredNames: string[] = [];

  // Register shared tools (work in both Node and Workers)
  for (const tool of sharedTools) {
    try {
      // Extract shape from schema, handling ZodEffects (refined schemas)
      const inputSchemaShape = getSchemaShape(tool.inputSchema);
      if (!inputSchemaShape) {
        logger.error('tools', {
          message: 'Failed to extract schema shape',
          toolName: tool.name,
        });
        throw new Error(`Failed to extract schema shape for tool: ${tool.name}`);
      }

      const wrappedHandler = createWrappedHandler(tool.handler);

      // Shared tools use Zod schemas - pass extracted shape for SDK compatibility
      server.registerTool(
        tool.name,
        {
          description: tool.description,
          inputSchema: inputSchemaShape,
          ...(tool.outputSchema && { outputSchema: tool.outputSchema }),
          ...(tool.annotations && { annotations: tool.annotations }),
        },
        wrappedHandler as Parameters<typeof server.registerTool>[2],
      );

      registeredNames.push(tool.name);
      logger.debug('tools', { message: 'Registered tool', toolName: tool.name });
    } catch (error) {
      logger.error('tools', {
        message: 'Failed to register tool',
        toolName: tool.name,
        error: (error as Error).message,
      });
      throw error;
    }
  }

  logger.info('tools', {
    message: `Registered ${registeredNames.length} tools`,
    toolNames: registeredNames,
  });
}

/**
 * Create a wrapped handler for shared tools.
 * Adapts the shared ToolContext to the SDK's RequestHandlerExtra.
 */
function createWrappedHandler(
  handler: (args: Record<string, unknown>, context: ToolContext) => Promise<unknown>,
) {
  return async (
    args: Record<string, unknown>,
    extra?: {
      requestId?: string | number;
      _meta?: { progressToken?: string | number };
      signal?: AbortSignal;
    },
  ) => {
    // SDK provides requestId at top level of extra
    const requestId = extra?.requestId;

    // Look up auth context from registry (stored by MCP routes with auth info)
    let existingContext = requestId ? contextRegistry.get(requestId) : undefined;

    // Fallback to AsyncLocalStorage if requestId not available
    // This is the primary method since MCP SDK doesn't pass requestId to tool handlers
    if (!existingContext) {
      existingContext = getCurrentAuthContext();
    }

    // Build shared ToolContext
    const context: ToolContext = {
      sessionId: String(requestId || crypto.randomUUID()),
      signal: extra?.signal,
      meta: {
        progressToken: extra?._meta?.progressToken,
        requestId: requestId ? String(requestId) : undefined,
      },
      // Auth from context registry
      authStrategy: existingContext?.authStrategy,
      providerToken: existingContext?.providerToken,
      provider: existingContext?.provider
        ? {
            accessToken: existingContext.provider.access_token,
            refreshToken: existingContext.provider.refresh_token,
            expiresAt: existingContext.provider.expires_at,
            scopes: existingContext.provider.scopes,
          }
        : undefined,
      resolvedHeaders: existingContext?.resolvedHeaders,
      authHeaders: existingContext?.authHeaders as Record<string, string> | undefined,
    };

    try {
      const result = await handler(args, context);
      return result;
    } finally {
      if (requestId) {
        contextRegistry.delete(requestId);
      }
    }
  };
}
