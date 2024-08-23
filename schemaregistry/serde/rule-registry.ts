import {RuleAction, RuleExecutor} from "./serde";

const ruleExecutors = new Map<string, RuleExecutor>

const ruleActions = new Map<string, RuleAction>


// registerRuleExecutor is used to register a new rule executor.
export function registerRuleExecutor(ruleExecutor: RuleExecutor): void {
  ruleExecutors.set(ruleExecutor.type(), ruleExecutor)
}

// getRuleExecutor fetches a rule executor by a given name.
export function getRuleExecutor(name: string): RuleExecutor | undefined {
  return ruleExecutors.get(name)
}

// getRuleExecutors fetches all rule executors
export function getRuleExecutors(): RuleExecutor[] {
  return Array.from(ruleExecutors.values())
}

// registerRuleAction is used to register a new rule action.
export function registerRuleAction(ruleAction: RuleAction): void {
  ruleActions.set(ruleAction.type(), ruleAction)
}

// getRuleAction fetches a rule action by a given name.
export function getRuleAction(name: string): RuleAction | undefined {
  return ruleActions.get(name)
}

// getRuleActions fetches all rule actions
export function getRuleActions(): RuleAction[] {
  return Array.from(ruleActions.values())
}

// clearRules clears all registered rules
export function clearRules(): void {
  ruleExecutors.clear()
  ruleActions.clear()
}
