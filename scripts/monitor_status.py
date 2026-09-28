"""Print Prometheus' active targets as a small table: make monitor-status.

Reads the JSON of /api/v1/targets on stdin. Standard library only.
"""
import json
import sys

targets = json.load(sys.stdin)["data"]["activeTargets"]
rows = sorted((t["labels"].get("stack", "?"), t["labels"].get("component", t["labels"]["job"]),
               t["health"], t["labels"]["instance"], t.get("lastError", ""))
              for t in targets)
print(f"{'STACK':<11}{'COMPONENT':<20}{'HEALTH':<8}ENDPOINT")
for stack, comp, health, inst, err in rows:
    print(f"{stack:<11}{comp:<20}{health:<8}{inst}" + (f"  ({err[:60]})" if err else ""))
up = sum(r[2] == "up" for r in rows)
print(f"\n{up}/{len(rows)} targets up. Down targets are stacks that are not started.")
