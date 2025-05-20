import {RuleRegistry} from "../../serde/rule-registry";
import {
  FieldContext,
  FieldRuleExecutor,
  FieldTransform,
  RuleContext,
} from "../../serde/serde";
import {ClientConfig} from "../../rest-service";
import {CelExecutor} from "./cel-executor";

export class CelFieldExecutor extends FieldRuleExecutor {
  executor: CelExecutor = new CelExecutor()

  static register(): CelFieldExecutor {
    const executor = new CelFieldExecutor()
    RuleRegistry.registerRuleExecutor(executor)
    return executor
  }

  configure(clientConfig: ClientConfig, config: Map<string, string>) {
    this.config = config
  }

  type(): string {
    return "CEL_FIELD"
  }

  override newTransform(ctx: RuleContext): FieldTransform {
    return new CelFieldExecutorTransform(this.executor)
  }

  async close(): Promise<void> {
  }
}

export class CelFieldExecutorTransform implements FieldTransform {
  private executor: CelExecutor

  constructor(executor: CelExecutor) {
    this.executor = executor
  }

  async transform(ctx: RuleContext, fieldCtx: FieldContext, fieldValue: any): Promise<any> {
    if (fieldValue == null) {
      return null
    }
    if (!fieldCtx.isPrimitive()) {
      return fieldValue
    }
    const args = {
      value: fieldValue,
      fullName: fieldCtx.fullName,
      name: fieldCtx.name,
      typeName: fieldCtx.typeName(),
      tags: Array.from(fieldCtx.tags),
      message: fieldCtx.containingMessage
    }
    return await this.executor.execute(ctx, fieldValue, args)
  }
}
