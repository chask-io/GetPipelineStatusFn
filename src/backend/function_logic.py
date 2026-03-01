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
    """Build execution order section showing tiers of nodes based on dependencies."""
    lines = ["## 📋 Orden de Ejecución Recomendado\n"]

    # Group nodes into execution tiers
    # Tier 1: Nodes with no prerequisites (can start immediately)
    # Tier 2: Nodes that depend on Tier 1, etc.

    tier_1_nodes = []
    waiting_nodes = []

    for node in nodes:
        node_id = node.get("id")
        prereqs = prerequisites_map.get(node_id, [])

        if not prereqs:
            tier_1_nodes.append(node)
        else:
            waiting_nodes.append(node)

    # Show Tier 1 nodes (can start now)
    if tier_1_nodes:
        lines.append("### ✅ Nodos Sin Prerequisitos (Puedes empezar con estos):")
        for node in tier_1_nodes:
            node_id = node.get("id")
            title = node.get("title")
            status = node.get("status")
            status_emoji = {"unassigned": "⚪", "assigned": "🔵", "in_progress": "🟡", "completed": "✅"}.get(status, "⚪")
            lines.append(f"- {status_emoji} **Nodo {node_id}**: {title}")

    # Show waiting nodes grouped by their dependencies
    if waiting_nodes:
        lines.append("\n### ⏳ Nodos Con Prerequisitos (Completar en orden):")

        # Group by prerequisite pattern
        dependency_groups = {}
        for node in waiting_nodes:
            node_id = node.get("id")
            prereqs = tuple(sorted(prerequisites_map.get(node_id, [])))  # Use tuple for dict key

            if prereqs not in dependency_groups:
                dependency_groups[prereqs] = []
            dependency_groups[prereqs].append(node)

        # Display each group
        for prereq_tuple, group_nodes in dependency_groups.items():
            prereq_list = list(prereq_tuple)

            # Show the prerequisite requirement
            if len(group_nodes) == 1:
                # Single node waiting
                node = group_nodes[0]
                node_id = node.get("id")
                title = node.get("title")
                status = node.get("status")
                status_emoji = {"unassigned": "⚪", "assigned": "🔵", "in_progress": "🟡", "completed": "✅"}.get(status, "⚪")

                # Count completed prerequisites
                completed_prereqs = sum(
                    1 for prereq_id in prereq_list
                    if any(n.get("id") == prereq_id and n.get("status") == "completed" for n in nodes)
                )
                total_prereqs = len(prereq_list)

                lines.append(f"\n{status_emoji} **Nodo {node_id}**: {title}")
                lines.append(f"   Requiere {total_prereqs} prerequisito(s) - **{completed_prereqs}/{total_prereqs} completados**")
                lines.append(f"   Debe completar primero: {', '.join([f'Nodo {p}' for p in prereq_list])}")

            else:
                # Multiple nodes with same prerequisites
                lines.append(f"\n**Grupo de {len(group_nodes)} nodos** que requieren los mismos prerequisitos:")

                # Count completed prerequisites
                completed_prereqs = sum(
                    1 for prereq_id in prereq_list
                    if any(n.get("id") == prereq_id and n.get("status") == "completed" for n in nodes)
                )
                total_prereqs = len(prereq_list)

                lines.append(f"   ⚠️  Prerequisitos ({completed_prereqs}/{total_prereqs} completados): {', '.join([f'Nodo {p}' for p in prereq_list])}")
                lines.append(f"   📌 Debes completar ✅ TODOS estos prerequisitos antes de operar cualquiera de:")

                for node in group_nodes:
                    node_id = node.get("id")
                    title = node.get("title")
                    status = node.get("status")
                    status_emoji = {"unassigned": "⚪", "assigned": "🔵", "in_progress": "🟡", "completed": "✅"}.get(status, "⚪")
                    lines.append(f"      - {status_emoji} Nodo {node_id}: {title}")

    lines.append("")  # Add blank line
    return lines
