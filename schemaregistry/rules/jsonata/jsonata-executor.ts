import {RuleRegistry} from "../../serde/rule-registry";
import {RuleContext, RuleExecutor} from "../../serde/serde";
import {ClientConfig} from "../../rest-service";
import {LRUCache} from "lru-cache";
import jsonata, {Expression} from "jsonata";

export class JsonataExecutor implements RuleExecutor {
  config: Map<string, string> | null = null
  cache: LRUCache<string, Expression> = new LRUCache({max: 1000})

  /**
   * Register the JSONata rule executor with the rule registry.
   */
  static register(): JsonataExecutor {
    const executor = new JsonataExecutor()
    RuleRegistry.registerRuleExecutor(executor)
    return executor
  }

  configure(clientConfig: ClientConfig, config: Map<string, string>) {
    this.config = config
  }

  type(): string {
    return "JSONATA"
  }

  async transform(ctx: RuleContext, msg: any): Promise<any> {
    let expr = ctx.rule.expr
    if (expr == null) {
      return msg
    }
    let jsonataExpr = this.cache.get(expr)
    if (jsonataExpr == null) {
      jsonataExpr = jsonata(expr)
      this.cache.set(expr, jsonataExpr)
    }
    return jsonataExpr.evaluate(msg)
  }

  async close(): Promise<void> {
  }
}
