import {RuleAction, RuleExecutor} from "./serde";

/**
 * 
 */
export class RuleRegistry {
  private ruleExecutors: Map<string, RuleExecutor> = new Map<string, RuleExecutor>()
  private ruleActions: Map<string, RuleAction> = new Map<string, RuleAction>()

  private static globalInstance: RuleRegistry = new RuleRegistry()

  /**
   * registerExecutor is used to register a new rule executor.
   * @param ruleExecutor - the rule executor to register
   */
  public registerExecutor(ruleExecutor: RuleExecutor): void {
    this.ruleExecutors.set(ruleExecutor.type(), ruleExecutor)
  }

  /**
   * getExecutor fetches a rule executor by a given name.
   * @param name - the name of the rule executor to fetch
   */
  public getExecutor(name: string): RuleExecutor | undefined {
    return this.ruleExecutors.get(name)
  }

  /**
   * getExecutors fetches all rule executors
   */
  public getExecutors(): RuleExecutor[] {
    return Array.from(this.ruleExecutors.values())
  }

  /**
   * registerAction is used to register a new rule action.
   * @param ruleAction - the rule action to register
   */
  public registerAction(ruleAction: RuleAction): void {
    this.ruleActions.set(ruleAction.type(), ruleAction)
  }

  /**
   * getAction fetches a rule action by a given name.
   * @param name - the name of the rule action to fetch
   */
  public getAction(name: string): RuleAction | undefined {
    return this.ruleActions.get(name)
  }

  /**
   * getActions fetches all rule actions
   */
  public getActions(): RuleAction[] {
    return Array.from(this.ruleActions.values())
  }

  /**
   * clear clears all registered rules
   */
  public clear(): void {
    this.ruleExecutors.clear()
    this.ruleActions.clear()
  }

  /**
   * getGlobalInstance fetches the global instance of the rule registry
   */
  public static getGlobalInstance(): RuleRegistry {
    return RuleRegistry.globalInstance
  }

  /**
   * registerRuleExecutor is used to register a new rule executor globally.
   * @param ruleExecutor - the rule executor to register
   */
  public static registerRuleExecutor(ruleExecutor: RuleExecutor): void {
    RuleRegistry.globalInstance.registerExecutor(ruleExecutor)
  }

  /**
   * registerRuleAction is used to register a new rule action globally.
   * @param ruleAction - the rule action to register
   */
  public static registerRuleAction(ruleAction: RuleAction): void {
    RuleRegistry.globalInstance.registerAction(ruleAction)
  }
}
