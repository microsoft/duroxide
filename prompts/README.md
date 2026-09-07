# AI Prompts for Duroxide Development

This directory contains structured prompts for LLMs working on the duroxide codebase. Each prompt provides comprehensive guidance for common development tasks.

## Available Prompts

### Core Development Tasks

1. **[duroxide-create-scenario-test.md](duroxide-create-scenario-test.md)**
   - Converting orchestrations to regression tests
   - Supporting Duroxide, Temporal, and Durable Tasks patterns
   - Creating scenario tests in `tests/scenarios/`
   - Modeling real-world orchestration patterns

### Quality and Maintenance

2. **[duroxide-clean-warnings.md](duroxide-clean-warnings.md)**
   - Eliminating compiler warnings
   - Running clippy
   - Proper handling of unused code
   - Code formatting

3. **[duroxide-update-docs-tests.md](duroxide-update-docs-tests.md)**
   - Comprehensive documentation review
   - Proposing tests for code changes
   - Metrics specification updates and OTel audits
   - Keeping guides accurate
   - API documentation standards
   - Example validation

### Performance

4. **[duroxide-profile.md](duroxide-profile.md)**
   - Profiling runtime performance
   - Flamegraph analysis
   - Identifying bottlenecks
   - Optimization strategies

5. **[duroxide-stress-test.md](duroxide-stress-test.md)**
   - Running comprehensive stress tests
   - Updating results files (local vs cloud)
   - Interpreting performance metrics
   - Tracking performance over time

### Git Operations

6. **[duroxide-merge-main.md](duroxide-merge-main.md)**
   - Committing changes
   - Merging branches
   - Writing good commit messages
   - Pushing to remote

## How to Use These Prompts

### For LLM Assistants
Reference the appropriate prompt when tackling a task:

```
@duroxide-create-scenario-test.md

Convert this Temporal workflow to a Duroxide scenario test: [workflow code]
```

### For Humans
Use these as checklists and guidelines when:
- Onboarding new contributors
- Planning complex changes
- Reviewing pull requests
- Establishing coding standards

## Prompt Design Principles

These prompts follow consistent patterns:

1. **Clear Objective** - What are we trying to accomplish?
2. **Structured Steps** - Ordered tasks to complete the objective
3. **Examples** - Show correct patterns vs anti-patterns
4. **Checklists** - Ensure nothing is missed
5. **Quality Gates** - Validation steps before considering done

## Adding New Prompts

When creating new prompts:

### Good Prompt Topics
- Implementing new provider (postgres, redis, etc.)
- Adding new orchestration pattern
- Performance profiling and optimization
- Security review and hardening
- Upgrading dependencies

### Prompt Template
```markdown
# Task Name

## Objective
Clear statement of what we're trying to accomplish

## Planning Phase
- Understand requirements
- Identify affected components
- Plan approach

## Implementation Phase
- Step-by-step instructions
- Code patterns and examples
- Common pitfalls to avoid

## Validation Phase
- Testing checklist
- Quality checks
- Documentation updates

## When to Ask
Scenarios where human input is needed
```

## Integration with Development Workflow

### Scenario Test Creation Cycle
```
1. @duroxide-create-scenario-test.md      - Convert orchestration to test
2. @duroxide-clean-warnings.md            - Clean up warnings
3. @duroxide-update-docs-tests.md         - Update documentation and add tests
4. @duroxide-merge-main.md                - Commit and merge
```

### Performance Optimization Cycle
```
1. @duroxide-profile.md         - Profile and identify bottlenecks
2. @duroxide-stress-test.md     - Run stress tests to validate
3. @duroxide-profile.md         - Re-profile to verify improvements
4. @duroxide-clean-warnings.md  - Clean up warnings
5. @duroxide-merge-main.md      - Commit and merge
```

### Documentation & Test Update Cycle
```
1. @duroxide-update-docs-tests.md  - Review docs, propose tests, audit metrics
2. @duroxide-clean-warnings.md     - Clean up any code issues found
3. @duroxide-merge-main.md         - Commit and merge
```

## Suggested Additional Prompts

Consider creating these prompts for specialized tasks:

- **duroxide-add-provider.md** - Implementing new storage backends (PostgreSQL, DynamoDB, Redis)
- **duroxide-add-example.md** - Creating comprehensive runnable examples
- **duroxide-review-pr.md** - Guidelines for reviewing pull requests
- **duroxide-benchmark.md** - Adding and running benchmarks
- **duroxide-security-review.md** - Security considerations and review checklist

## Quality Standards for Prompts

All prompts should:
- Be actionable (clear steps, not vague suggestions)
- Include examples (both good and bad patterns)
- Have validation steps (how to know you're done)
- Reference actual code when possible
- Be maintainable (update as codebase evolves)

## Feedback Loop

If a prompt is frequently insufficient:
1. Note what questions came up
2. Note what was unclear
3. Update the prompt with those answers
4. Add examples addressing the confusion

## Version Control for Prompts

- Prompts should evolve with the codebase
- When making breaking changes to duroxide, update affected prompts
- Keep prompts in sync with ORCHESTRATION-GUIDE.md and other docs
- Version prompts if we ever publish them separately
