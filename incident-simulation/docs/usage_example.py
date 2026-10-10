"""
OpsPulse AI Incident Simulation Framework - Usage Example
=========================================================

This script demonstrates how to load and use the incident simulation framework
for testing OpsPulse AI's incident detection and diagnostic capabilities.

Principal SRE-grade incident simulation framework with:
- 10 realistic Kubernetes cluster incidents
- LangGraph multi-agent diagnostic traces
- Confidence scoring and remediation recommendations
- Interactive Streamlit visualization
"""

import json
from pathlib import Path
from typing import Dict, List, Any


class IncidentSimulationFramework:
    """Main class for interacting with the incident simulation framework."""

    def __init__(self, base_path: str = "incident-simulation"):
        self.base_path = Path(base_path)
        self.data_path = self.base_path / "data"
        self.traces_path = self.base_path / "traces"

    def list_incidents(self) -> List[str]:
        """List all available incident IDs."""
        incident_files = self.data_path.glob("incident_*.json")
        return [f.stem for f in sorted(incident_files)]

    def load_incident(self, incident_id: str) -> Dict[str, Any]:
        """Load incident data by ID."""
        incident_file = self.data_path / f"{incident_id}.json"
        if not incident_file.exists():
            raise FileNotFoundError(f"Incident {incident_id} not found")

        with open(incident_file, 'r') as f:
            return json.load(f)

    def load_trace(self, incident_id: str) -> Dict[str, Any]:
        """Load diagnostic trace by incident ID."""
        trace_id = incident_id.replace("incident", "trace")
        trace_file = self.traces_path / f"{trace_id}.json"
        if not trace_file.exists():
            raise FileNotFoundError(f"Trace for {incident_id} not found")

        with open(trace_file, 'r') as f:
            return json.load(f)

    def get_incident_summary(self, incident_id: str) -> str:
        """Get a human-readable summary of an incident."""
        incident = self.load_incident(incident_id)
        trace = self.load_trace(incident_id)

        summary = f"""
Incident ID: {incident['incident_id']}
Type: {incident['incident_type']}
Time: {incident['timestamp_start']} to {incident['timestamp_end']}
Cluster: {incident['cluster']}
Namespace: {incident['namespace']}
Affected Pods: {len(incident['affected_pods'])}

Log Entries: {len(incident['log_stream'])}
Metrics: {len(incident['metrics_stream'])}

Expected Agents: {', '.join(trace['langgraph_diagnostic_trace']['agents_involved'])}
Overall Confidence: {trace['overall_confidence']:.2f}

Root Cause: {trace['langgraph_diagnostic_trace']['diagnostic_steps'][-1]['root_cause']}
"""
        return summary

    def benchmark_detection(self, incident_id: str, detected_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Benchmark OpsPulse AI detection against expected trace.

        Args:
            incident_id: The incident ID to test
            detected_result: The result from OpsPulse AI analysis

        Returns:
            Benchmark results with accuracy metrics
        """
        expected_trace = self.load_trace(incident_id)
        expected_agents = set(expected_trace['langgraph_diagnostic_trace']['agents_involved'])
        expected_confidence = expected_trace['overall_confidence']

        # Calculate metrics
        detected_agents = set(detected_result.get('agents', []))
        detected_confidence = detected_result.get('confidence', 0.0)

        agent_match = expected_agents == detected_agents
        confidence_diff = abs(expected_confidence - detected_confidence)
        confidence_match = confidence_diff < 0.1

        return {
            'incident_id': incident_id,
            'agent_match': agent_match,
            'confidence_match': confidence_match,
            'confidence_difference': confidence_diff,
            'expected_agents': expected_agents,
            'detected_agents': detected_agents,
            'expected_confidence': expected_confidence,
            'detected_confidence': detected_confidence,
            'overall_score': (1.0 if agent_match else 0.0) * (1.0 if confidence_match else 0.0)
        }

    def get_remediation_commands(self, incident_id: str) -> List[Dict[str, str]]:
        """Extract remediation commands from trace."""
        trace = self.load_trace(incident_id)
        steps = trace['langgraph_diagnostic_trace']['diagnostic_steps']

        # Find the remediation step
        for step in steps:
            if step.get('agent') == 'RemediationAdvisor':
                return step.get('recommendations', [])

        return []


def main():
    """Example usage of the incident simulation framework."""

    # Initialize framework
    framework = IncidentSimulationFramework()

    # List all available incidents
    print("Available Incidents:")
    print("=" * 50)
    for incident_id in framework.list_incidents():
        print(f"  - {incident_id}")
    print()

    # Load and analyze a specific incident
    incident_id = "incident_01_cascading_oom"
    print(f"Analyzing {incident_id}:")
    print("=" * 50)
    print(framework.get_incident_summary(incident_id))

    # Get remediation commands
    print("\nRemediation Commands:")
    print("=" * 50)
    commands = framework.get_remediation_commands(incident_id)
    for i, cmd in enumerate(commands, 1):
        print(f"\n{i}. Priority: {cmd['priority']}")
        print(f"   Command: {cmd['command'][:80]}...")
        print(f"   Rationale: {cmd['rationale']}")

    # Example benchmark test (mock result)
    print("\n\nBenchmark Test Example:")
    print("=" * 50)
    mock_detected_result = {
        'agents': ['LogAnalyzer', 'MetricsCorrelator', 'TopologyMapper', 'RootCauseAnalyzer', 'RemediationAdvisor'],
        'confidence': 0.93
    }

    benchmark_result = framework.benchmark_detection(incident_id, mock_detected_result)
    print(f"Agent Match: {benchmark_result['agent_match']}")
    print(f"Confidence Match: {benchmark_result['confidence_match']}")
    print(f"Confidence Difference: {benchmark_result['confidence_difference']:.3f}")
    print(f"Overall Score: {benchmark_result['overall_score']:.2f}")


if __name__ == "__main__":
    main()
