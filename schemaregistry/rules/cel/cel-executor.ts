import {RuleRegistry} from "../../serde/rule-registry";
import {RuleContext, RuleExecutor} from "../../serde/serde";
import {ClientConfig} from "../../rest-service";
import stringify from "json-stringify-deterministic";
import {LRUCache} from "lru-cache";
import {celEnv, parse, plan} from "@bufbuild/cel";
import { STRINGS_EXT_FUNCS } from "@bufbuild/cel/ext/strings";

export class CelExecutor implements RuleExecutor {
  config: Map<string, string> | null = null
  env = celEnv({ funcs: STRINGS_EXT_FUNCS });
  cache: LRUCache<string, any> = new LRUCache({max: 1000})

  static register(): CelExecutor {
    const executor = new CelExecutor()
    RuleRegistry.registerRuleExecutor(executor)
    return executor
  }

  configure(clientConfig: ClientConfig, config: Map<string, string>) {
    this.config = config
  }

  type(): string {
    return "CEL"
  }

  async transform(ctx: RuleContext, msg: any): Promise<any> {
    const args = {
      message: msg
    }
    return await this.execute(ctx, msg, args)
  }

  async execute(ctx: RuleContext, msg: any, args: { [key: string]: any }): Promise<any> {
    let expr = ctx.rule.expr
    if (expr == null) {
      return msg
    }
    const index = expr.indexOf(';')
    if (index >= 0) {
      const guard = expr.substring(0, index)
      if (guard.trim().length != 0) {
        const guardResult = await this.executeRule(ctx, guard, msg, args)
        if (guardResult === false) {
          // skip the expr
          if (ctx.rule.kind === 'CONDITION') {
            return true
          }
          return msg
        }
      }
      expr = expr.substring(index + 1)
    }
    return await this.executeRule(ctx, expr, msg, args)
  }

  async executeRule(ctx: RuleContext, expr: string, obj: any, args: { [key: string]: any }): Promise<any> {
    const schema = ctx.target.schema
    const scriptType = ctx.target.schemaType
    const rule: RuleWithArgs = {
      rule: expr,
      scriptType: scriptType,
      schema: schema
    }
    const ruleJson = stringify(rule)
    let program = this.cache.get(ruleJson)
    if (program == null) {
      const parsedExpr = parse(expr)
      program = plan(this.env, parsedExpr)
      this.cache.set(ruleJson, program)
    }
    return program(args)
  }

  async close(): Promise<void> {
  }
}

interface RuleWithArgs {
  rule?: string
  scriptType?: string
  schema?: string
}
