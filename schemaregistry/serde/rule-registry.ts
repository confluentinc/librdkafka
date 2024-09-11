import {RuleAction, RuleExecutor} from "./serde";

export class RuleRegistry {
  private ruleExecutors: Map<string, RuleExecutor> = new Map<string, RuleExecutor>()
  private ruleActions: Map<string, RuleAction> = new Map<string, RuleAction>()

  private static globalInstance: RuleRegistry = new RuleRegistry()

  // registerExecutor is used to register a new rule executor.
  public registerExecutor(ruleExecutor: RuleExecutor): void {
    this.ruleExecutors.set(ruleExecutor.type(), ruleExecutor)
  }

  // getExecutor fetches a rule executor by a given name.
  public getExecutor(name: string): RuleExecutor | undefined {
    return this.ruleExecutors.get(name)
  }

  // getExecutors fetches all rule executors
  public getExecutors(): RuleExecutor[] {
    return Array.from(this.ruleExecutors.values())
  }

  // registerAction is used to register a new rule action.
  public registerAction(ruleAction: RuleAction): void {
    this.ruleActions.set(ruleAction.type(), ruleAction)
  }

  // getAction fetches a rule action by a given name.
  public getAction(name: string): RuleAction | undefined {
    return this.ruleActions.get(name)
  }

  // getActions fetches all rule actions
  public getActions(): RuleAction[] {
    return Array.from(this.ruleActions.values())
  }

  // clear clears all registered rules
  public clear(): void {
    this.ruleExecutors.clear()
    this.ruleActions.clear()
  }

  public static getGlobalInstance(): RuleRegistry {
    return RuleRegistry.globalInstance
  }

  public static registerRuleExecutor(ruleExecutor: RuleExecutor): void {
    RuleRegistry.globalInstance.registerExecutor(ruleExecutor)
  }

  public static registerRuleAction(ruleAction: RuleAction): void {
    RuleRegistry.globalInstance.registerAction(ruleAction)
  }
}
