"""Boundary contracts: Simulation never receives physical payloads."""

from sqlalchemy import create_engine

from app.simulation_engine import SimulationEngine
from app.simulation_migrations import migrate
from app.simulator_store import ScenarioCreate, SimulatorStore


def test_simulation_rejects_physical_payload_models_and_dataset_references():
    database = create_engine("sqlite+pysqlite:///:memory:")
    migrate(database)
    store = SimulatorStore(database)
    scenario = store.create_scenario(ScenarioCreate(
        name="logical-only", fidelity="DATA", seed="seed", logical_size_bytes=100,
    ))
    engine = SimulationEngine(store)
    for kwargs in (
        {"payload_model": "REPRESENTATIVE", "payload_dataset_id": "dataset"},
        {"payload_model": "HYBRID", "payload_dataset_id": "dataset", "physical_object_indices": [0]},
        {"payload_model": "VIRTUAL", "payload_dataset_id": "dataset"},
    ):
        try:
            engine.materialize(
                scenario.id, source_bucket="source", destination_bucket="destination",
                region="us-east-1", object_count=1, logical_size_bytes=100,
                prefixes=["simulation"], storage_class="STANDARD", **kwargs,
            )
            raise AssertionError("SIMULATION accepted a physical payload request")
        except ValueError as error:
            assert "Fujin LOCAL" in str(error)


def test_simulation_deterministic_materialization_needs_no_dataset_or_mount():
    database = create_engine("sqlite+pysqlite:///:memory:")
    migrate(database)
    store = SimulatorStore(database)
    scenario = store.create_scenario(ScenarioCreate(
        name="deterministic", fidelity="DATA", seed="seed", logical_size_bytes=100,
    ))
    result = SimulationEngine(store).materialize(
        scenario.id, source_bucket="source", destination_bucket="destination",
        region="us-east-1", object_count=2, logical_size_bytes=100,
        prefixes=["simulation"], storage_class="STANDARD",
    )
    assert result["payload_model"] == "VIRTUAL"
    assert result["physical_object_count"] == 0
