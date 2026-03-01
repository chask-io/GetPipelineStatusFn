"""
Business logic for GetPipelineStatusFn.

Obtiene el estado actual del pipeline mostrando todos los nodos,
la posición actual, funciones disponibles por nodo y el flujo entre nodos.

The handler.py file is infrastructure code and should NOT be modified.
"""

import logging
from typing import Any, Dict, List

from chask_foundation.backend.models import OrchestrationEvent
from api.pipeline_requests import pipeline_api_manager  # type: ignore[import]

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class FunctionBackend:
    """Backend for GetPipelineStatusFn."""

    def __init__(self, orchestration_event: OrchestrationEvent):
        self.orchestration_event = orchestration_event
        logger.info(
            "Initialized GetPipelineStatusFn for org: %s",
            orchestration_event.organization.organization_id,
        )

    def process_request(self) -> str:
        """Get pipeline status and return formatted markdown."""
        try:
            response = pipeline_api_manager.call(
                "get_pipeline_status",
                orchestration_session_uuid=self.orchestration_event.orchestration_session_uuid,
                **self._api_credentials(),
            )
            return _format_pipeline_status(response)
        except Exception as e:
            return f"Error al obtener el estado del pipeline: {str(e)}"

    def _api_credentials(self) -> dict:
        return {
            "access_token": self.orchestration_event.access_token,
            "organization_id": self.orchestration_event.organization.organization_id,
        }


# ---------------------------------------------------------------------------
# Formatting helpers (migrated from operator_llm pipeline_tools.py)
# ---------------------------------------------------------------------------

def _format_pipeline_status(data: dict) -> str:
    """Format pipeline status data as readable markdown."""
    pipeline_title = data.get("pipeline_title", "Pipeline")
    pipeline_desc = data.get("pipeline_description", "")
    current_node_id = data.get("current_node_id")
    nodes = data.get("nodes", [])
    edges = data.get("edges", [])

    # Build prerequisite map for each node (source -> target means source is prerequisite of target)
    prerequisites_map = {}  # node_id -> list of prerequisite node_ids
    for node in nodes:
        node_id = node.get("id")
        prerequisites_map[node_id] = []
        for edge in edges:
            if edge.get("target") == node_id:
                prerequisites_map[node_id].append(edge.get("source"))

    # Build markdown output
    lines = [
        f"# Pipeline: {pipeline_title}",
        f"{pipeline_desc}\n" if pipeline_desc else "",
    ]

    # Add execution order section BEFORE node details
    lines.extend(_build_execution_order_section(nodes, edges, prerequisites_map))

    lines.append("\n## Detalles de Nodos:")
    lines.append("_Información detallada de cada nodo del pipeline_\n")

    for node in nodes:
        node_id = node.get("id")
        title = node.get("title")
        status = node.get("status")
        node_type = node.get("node_type")
        is_current = node.get("is_current", False)

        # Mark current node
        current_marker = " **← YOU ARE HERE**" if is_current else ""
        status_emoji = {
            "unassigned": "⚪",
            "assigned": "🔵",
            "in_progress": "🟡",
            "completed": "✅"
        }.get(status, "⚪")

        lines.append(f"\n### {status_emoji} Node {node_id}: {title}{current_marker}")
        lines.append(f"- **Status**: {status}")
        lines.append(f"- **Type**: {node_type}")

        # Show prerequisites (incoming edges) - CRITICAL for dependency order
        prerequisite_ids = prerequisites_map.get(node_id, [])

        if prerequisite_ids:
            # Count completed vs incomplete
            completed_count = 0
            prerequisite_details = []

            for prereq_id in prerequisite_ids:
                prereq_node = next((n for n in nodes if n.get("id") == prereq_id), None)
                if prereq_node:
                    prereq_status = prereq_node.get("status")
                    prereq_title = prereq_node.get("title")
                    status_mark = "✅" if prereq_status == "completed" else "❌"
                    prerequisite_details.append(f"{status_mark} Nodo {prereq_id}: \"{prereq_title}\"")
                    if prereq_status == "completed":
                        completed_count += 1

            total_prereqs = len(prerequisite_ids)
            lines.append(f"- **Prerequisitos ({completed_count}/{total_prereqs} completados)** - DEBEN estar ✅ TODOS completados:")
            for prereq in prerequisite_details:
                lines.append(f"  - {prereq}")

        # Show functions if available
        functions = node.get("functions", [])
        if functions:
            lines.append("- **Functions**:")
            for func in functions:
                func_name = func.get("alias") or func.get("function_name")
                lines.append(f"  - {func_name}")

        # Show analyst if available
        analyst = node.get("analyst")
        if analyst:
            analyst_name = analyst.get("display_name") or analyst.get("name")
            lines.append(f"- **Analyst**: {analyst_name}")

    # Show flow connections grouped by destination node
    if edges:
        lines.append("\n## Flujo de Dependencias:")
        lines.append("_Muestra qué nodos deben completarse antes de cada nodo_\n")

        # Group edges by target (destination) node
        dependencies_by_target = {}
        for edge in edges:
            source = edge.get("source")
            target = edge.get("target")
            if target not in dependencies_by_target:
                dependencies_by_target[target] = []
            dependencies_by_target[target].append(source)

        # Sort by target node ID
        sorted_targets = sorted(dependencies_by_target.keys(), key=lambda x: int(x) if x.isdigit() else x)

        for target in sorted_targets:
            source_nodes = dependencies_by_target[target]

            # Get target node title for better context
            target_node = next((n for n in nodes if n.get("id") == target), None)
            target_title = target_node.get("title", f"Nodo {target}") if target_node else f"Nodo {target}"

            # Show the dependency relationship
            if len(source_nodes) == 1:
                lines.append(f"**{target_title}** (Nodo {target}) requiere:")
                lines.append(f"  - Nodo {source_nodes[0]}")
            else:
                lines.append(f"**{target_title}** (Nodo {target}) requiere TODOS estos nodos:")
                for source in source_nodes:
                    lines.append(f"  - Nodo {source}")

            lines.append("")  # Blank line between groups

    # Add navigation hint
    if current_node_id:
        lines.append(f"\n**Nodo Actual**: {current_node_id}")
        lines.append("Usa `OperateNode` para moverte a un nodo diferente.")
    else:
        lines.append("\n**Aún no hay nodo seleccionado.** Usa `OperateNode` para comenzar en un nodo específico.")

    return "\n".join(lines)


def _build_execution_order_section(nodes: list, edges: list, prerequisites_map: dict) -> list:
    """Build execution order using topological sort (Kahn's algorithm)."""
    lines = ["## 📋 Orden de Ejecución Recomendado\n"]

    node_by_id = {n.get("id"): n for n in nodes}
    all_node_ids = set(node_by_id.keys())

    # Kahn's algorithm: compute tiers by BFS layers
    in_degree = {nid: len(prerequisites_map.get(nid, [])) for nid in all_node_ids}
    tiers = []
    processed = set()

    while True:
        tier = [nid for nid in all_node_ids - processed if in_degree[nid] == 0]
        if not tier:
            break
        tiers.append(tier)
        for nid in tier:
            processed.add(nid)
            for other_id in all_node_ids - processed:
                if nid in prerequisites_map.get(other_id, []):
                    in_degree[other_id] -= 1

    # Any remaining nodes are in cycles (shouldn't happen but handle gracefully)
    remaining = all_node_ids - processed
    if remaining:
        tiers.append(list(remaining))

    # Render each tier
    for tier_num, tier_node_ids in enumerate(tiers, 1):
        tier_nodes = [node_by_id[nid] for nid in tier_node_ids if nid in node_by_id]

        if tier_num == 1:
            lines.append(f"### Paso {tier_num}: Nodos Sin Prerequisitos (Empezar aquí)")
        else:
            prereq_tiers = ", ".join([str(t) for t in range(1, tier_num)])
            lines.append(f"\n### Paso {tier_num}: Requiere completar paso(s) {prereq_tiers}")

        for node in tier_nodes:
            node_id = node.get("id")
            title = node.get("title")
            status = node.get("status")
            emoji = {"unassigned": "⚪", "assigned": "🔵", "in_progress": "🟡", "completed": "✅"}.get(status, "⚪")
            prereqs = prerequisites_map.get(node_id, [])
            dep_str = f" ← requiere: {', '.join(f'Nodo {p}' for p in prereqs)}" if prereqs else ""
            lines.append(f"- {emoji} **Nodo {node_id}**: {title}{dep_str}")

    lines.append("")
    return lines
