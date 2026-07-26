# Start here

Welcome. Spend ~15–20 minutes on this path and you will be able to explain Pulseforge accurately.

## 1. Orientation (5 min)

| Read | Why |
|---|---|
| [README.md](../README.md) | What the repo is |
| [CURRENT_STATE.md](../CURRENT_STATE.md) | Version, sprint, next task, blockers |
| [PROJECT_CONTEXT.md](../PROJECT_CONTEXT.md) | AI/human contributor contract |

## 2. Product philosophy (5 min)

| Read | Why |
|---|---|
| [vision/Mission.md](vision/Mission.md) | Why we exist |
| [vision/Product_Thesis.md](vision/Product_Thesis.md) | Core bet |
| [vision/Product_Constitution.md](vision/Product_Constitution.md) | Non-negotiables |
| [vision/Glossary.md](vision/Glossary.md) | Shared vocabulary |

Optional depth: [Product_Experience.md](vision/Product_Experience.md), [Intelligence_Architecture.md](vision/Intelligence_Architecture.md), [Product_Roadmap.md](vision/Product_Roadmap.md).

## 3. Engineering shape (5 min)

| Read | Why |
|---|---|
| [architecture/System_Architecture.md](architecture/System_Architecture.md) | Runtime topology |
| [architecture/Agent_Architecture.md](architecture/Agent_Architecture.md) | Agent boundaries |
| [architecture/Data_Architecture.md](architecture/Data_Architecture.md) | Core tables and tenancy |
| [adr/README.md](adr/README.md) | Why key choices were locked |

## 4. What to build next (2 min)

| Read | Why |
|---|---|
| [specs/README.md](specs/README.md) | Spec process |
| Active spec linked from CURRENT_STATE | Exact contract |
| [releases/](releases/) | Version intent |

---

## Hierarchy rules

```text
vision/          → why / what (product)
architecture/    → how (engineering)
specs/           → what we implement now
adr/             → why we chose this design
releases/        → when capability ships
CURRENT_STATE.md → what is true this week
```

Do not put sprint status in vision docs. Do not redefine product philosophy inside a PR description.

## After onboarding

1. Confirm CURRENT_STATE **Current Spec**.
2. Follow [CONTRIBUTING.md](../CONTRIBUTING.md).
3. For agent failure modes, consult `AGENT_RULES.md` and `CLAUDE.md`.
