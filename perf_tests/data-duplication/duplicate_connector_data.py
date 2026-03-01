#!/usr/bin/env python3
"""
Neo4j Connector Data Duplication Script

Duplicates RecordGroups, Records, type-specific nodes, and their relationships
for a given connector to enable load testing with larger datasets.

Usage:
    python -m scripts.duplicate_connector_data --connector-id <id> --copies <n>
    python -m scripts.duplicate_connector_data --connector-id <id> --copies <n> --dry-run
"""

import argparse
import asyncio
import logging
import os
import sys
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.graph_db.neo4j.neo4j_client import Neo4jClient


def setup_logging() -> logging.Logger:
    """Configure logging for the script."""
    logger = logging.getLogger("duplicate_connector_data")
    logger.setLevel(logging.INFO)
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger


class ConnectorDataDuplicator:
    """Duplicates connector data in Neo4j for load testing."""

    # Type-specific node labels that are connected to Records via IS_OF_TYPE
    TYPE_NODE_LABELS = ["File", "Mail", "Webpage", "Comment", "Ticket", "Link", "Project"]
    
    # Metadata relationship types
    METADATA_REL_TYPES = [
        "BELONGS_TO_DEPARTMENT",
        "BELONGS_TO_CATEGORY", 
        "BELONGS_TO_LANGUAGE",
        "BELONGS_TO_TOPIC"
    ]

    def __init__(
        self,
        client: Neo4jClient,
        connector_id: str,
        copies: int,
        batch_size: int,
        logger: logging.Logger
    ) -> None:
        self.client = client
        self.connector_id = connector_id
        self.copies = copies
        self.batch_size = batch_size
        self.logger = logger
        
        # Data containers
        self.record_groups: List[Dict] = []
        self.records: List[Dict] = []
        self.type_nodes: Dict[str, List[Dict]] = {}  # label -> nodes
        
        # Relationship containers
        self.rg_belongs_to_rg: List[Dict] = []  # RecordGroup -> RecordGroup
        self.rg_belongs_to_app: List[Dict] = []  # RecordGroup -> App BELONGS_TO (root RGs)
        self.record_belongs_to_rg: List[Dict] = []  # Record -> RecordGroup
        self.record_is_of_type: List[Dict] = []  # Record -> TypeNode
        self.record_relations: List[Dict] = []  # Record -> Record
        self.record_inherit_perms: List[Dict] = []  # Record -> RecordGroup
        self.rg_permissions: List[Dict] = []  # User/Group/Role -> RecordGroup
        self.record_metadata: List[Dict] = []  # Record -> Department/Category/etc
        
        # Statistics
        self.stats = {
            "record_groups_created": 0,
            "records_created": 0,
            "type_nodes_created": 0,
            "relationships_created": 0
        }

    async def run(self, dry_run: bool = False) -> Dict[str, int]:
        """
        Execute the duplication process.
        
        Args:
            dry_run: If True, only fetch and report counts without creating data
            
        Returns:
            Dictionary with creation statistics
        """
        start_time = time.time()
        
        self.logger.info(f"Starting data duplication for connector: {self.connector_id}")
        self.logger.info(f"Copies to create: {self.copies}")
        self.logger.info(f"Batch size: {self.batch_size}")
        self.logger.info(f"Dry run: {dry_run}")
        
        # Phase 1: Fetch original data
        self.logger.info("\n=== Phase 1: Fetching original data ===")
        await self._fetch_original_data()
        
        self._print_original_data_summary()
        
        if dry_run:
            self._print_dry_run_projection()
            return self.stats
        
        # Phase 2-4: Create copies
        for copy_num in range(1, self.copies + 1):
            self.logger.info(f"\n=== Creating copy {copy_num}/{self.copies} ===")
            
            # Create ID mappings for this copy
            mappings = self._create_id_mappings(copy_num)
            
            # Duplicate nodes
            await self._duplicate_nodes(copy_num, mappings)
            
            # Duplicate relationships
            await self._duplicate_relationships(copy_num, mappings)
            
            self.logger.info(f"Completed copy {copy_num}/{self.copies}")
        
        elapsed = time.time() - start_time
        self.logger.info(f"\n=== Duplication complete in {elapsed:.2f}s ===")
        self._print_final_statistics()
        
        return self.stats

    async def _fetch_original_data(self) -> None:
        """Fetch all original data for the connector."""
        await self._fetch_record_groups()
        await self._fetch_records()
        await self._fetch_type_nodes()
        await self._fetch_relationships()

    async def _fetch_record_groups(self) -> None:
        """Fetch all RecordGroups for the connector."""
        query = """
        MATCH (rg:RecordGroup {connectorId: $connector_id})
        RETURN rg
        """
        results = await self.client.execute_query(
            query, 
            {"connector_id": self.connector_id}
        )
        self.record_groups = [dict(r["rg"]) for r in results]
        self.logger.info(f"Fetched {len(self.record_groups)} RecordGroups")

    async def _fetch_records(self) -> None:
        """Fetch all Records for the connector."""
        query = """
        MATCH (r:Record {connectorId: $connector_id})
        RETURN r
        """
        results = await self.client.execute_query(
            query,
            {"connector_id": self.connector_id}
        )
        self.records = [dict(r["r"]) for r in results]
        self.logger.info(f"Fetched {len(self.records)} Records")

    async def _fetch_type_nodes(self) -> None:
        """Fetch all type-specific nodes connected to the connector's records."""
        for label in self.TYPE_NODE_LABELS:
            query = f"""
            MATCH (r:Record {{connectorId: $connector_id}})-[:IS_OF_TYPE]->(t:{label})
            RETURN DISTINCT t
            """
            results = await self.client.execute_query(
                query,
                {"connector_id": self.connector_id}
            )
            nodes = [dict(r["t"]) for r in results]
            if nodes:
                self.type_nodes[label] = nodes
                self.logger.info(f"Fetched {len(nodes)} {label} nodes")

    async def _fetch_relationships(self) -> None:
        """Fetch all relationships to be duplicated."""
        await self._fetch_rg_hierarchy()
        await self._fetch_rg_belongs_to_app()
        await self._fetch_record_belongs_to()
        await self._fetch_is_of_type()
        await self._fetch_record_relations()
        await self._fetch_inherit_permissions()
        await self._fetch_rg_permissions()
        await self._fetch_metadata_edges()

    async def _fetch_rg_hierarchy(self) -> None:
        """Fetch RecordGroup -> RecordGroup BELONGS_TO relationships."""
        query = """
        MATCH (child:RecordGroup {connectorId: $cid})-[rel:BELONGS_TO]->(parent:RecordGroup {connectorId: $cid})
        RETURN child.id AS childId, parent.id AS parentId, properties(rel) AS props
        """
        results = await self.client.execute_query(
            query,
            {"cid": self.connector_id}
        )
        self.rg_belongs_to_rg = [
            {"childId": r["childId"], "parentId": r["parentId"], "props": r["props"] or {}}
            for r in results
        ]
        self.logger.info(f"Fetched {len(self.rg_belongs_to_rg)} RecordGroup hierarchy edges")

    async def _fetch_rg_belongs_to_app(self) -> None:
        """Fetch RecordGroup -> App BELONGS_TO relationships (root RecordGroups)."""
        query = """
        MATCH (rg:RecordGroup {connectorId: $cid})-[rel:BELONGS_TO]->(app:App)
        RETURN rg.id AS rgId, app.id AS appId, properties(rel) AS props
        """
        results = await self.client.execute_query(
            query,
            {"cid": self.connector_id}
        )
        self.rg_belongs_to_app = [
            {"rgId": r["rgId"], "appId": r["appId"], "props": r["props"] or {}}
            for r in results
        ]
        self.logger.info(f"Fetched {len(self.rg_belongs_to_app)} RecordGroup->App edges")

    async def _fetch_record_belongs_to(self) -> None:
        """Fetch Record -> RecordGroup BELONGS_TO relationships."""
        query = """
        MATCH (r:Record {connectorId: $cid})-[rel:BELONGS_TO]->(rg:RecordGroup {connectorId: $cid})
        RETURN r.id AS recordId, rg.id AS rgId, properties(rel) AS props
        """
        results = await self.client.execute_query(
            query,
            {"cid": self.connector_id}
        )
        self.record_belongs_to_rg = [
            {"recordId": r["recordId"], "rgId": r["rgId"], "props": r["props"] or {}}
            for r in results
        ]
        self.logger.info(f"Fetched {len(self.record_belongs_to_rg)} Record->RecordGroup edges")

    async def _fetch_is_of_type(self) -> None:
        """Fetch Record -> TypeNode IS_OF_TYPE relationships."""
        type_labels = "|".join(self.TYPE_NODE_LABELS)
        query = f"""
        MATCH (r:Record {{connectorId: $cid}})-[rel:IS_OF_TYPE]->(t)
        WHERE any(label IN labels(t) WHERE label IN $type_labels)
        RETURN r.id AS recordId, t.id AS typeId, 
               head([l IN labels(t) WHERE l IN $type_labels]) AS typeLabel,
               properties(rel) AS props
        """
        results = await self.client.execute_query(
            query,
            {"cid": self.connector_id, "type_labels": self.TYPE_NODE_LABELS}
        )
        self.record_is_of_type = [
            {
                "recordId": r["recordId"],
                "typeId": r["typeId"],
                "typeLabel": r["typeLabel"],
                "props": r["props"] or {}
            }
            for r in results
        ]
        self.logger.info(f"Fetched {len(self.record_is_of_type)} IS_OF_TYPE edges")

    async def _fetch_record_relations(self) -> None:
        """Fetch Record -> Record RECORD_RELATION relationships."""
        query = """
        MATCH (r1:Record {connectorId: $cid})-[rel:RECORD_RELATION]->(r2:Record {connectorId: $cid})
        RETURN r1.id AS fromId, r2.id AS toId, properties(rel) AS props
        """
        results = await self.client.execute_query(
            query,
            {"cid": self.connector_id}
        )
        self.record_relations = [
            {"fromId": r["fromId"], "toId": r["toId"], "props": r["props"] or {}}
            for r in results
        ]
        self.logger.info(f"Fetched {len(self.record_relations)} RECORD_RELATION edges")

    async def _fetch_inherit_permissions(self) -> None:
        """Fetch Record -> RecordGroup INHERIT_PERMISSIONS relationships."""
        query = """
        MATCH (r:Record {connectorId: $cid})-[rel:INHERIT_PERMISSIONS]->(rg:RecordGroup {connectorId: $cid})
        RETURN r.id AS recordId, rg.id AS rgId, properties(rel) AS props
        """
        results = await self.client.execute_query(
            query,
            {"cid": self.connector_id}
        )
        self.record_inherit_perms = [
            {"recordId": r["recordId"], "rgId": r["rgId"], "props": r["props"] or {}}
            for r in results
        ]
        self.logger.info(f"Fetched {len(self.record_inherit_perms)} INHERIT_PERMISSIONS edges")

    async def _fetch_rg_permissions(self) -> None:
        """Fetch permission edges to RecordGroups (from Users, Groups, Roles, Organizations)."""
        query = """
        MATCH (source)-[rel:PERMISSION]->(rg:RecordGroup {connectorId: $cid})
        RETURN source.id AS sourceId, labels(source) AS sourceLabels, 
               rg.id AS rgId, properties(rel) AS props
        """
        results = await self.client.execute_query(
            query,
            {"cid": self.connector_id}
        )
        self.rg_permissions = [
            {
                "sourceId": r["sourceId"],
                "sourceLabels": r["sourceLabels"],
                "rgId": r["rgId"],
                "props": r["props"] or {}
            }
            for r in results
        ]
        self.logger.info(f"Fetched {len(self.rg_permissions)} PERMISSION edges to RecordGroups")

    async def _fetch_metadata_edges(self) -> None:
        """Fetch metadata edges from Records (department, category, language, topic)."""
        rel_pattern = "|".join(self.METADATA_REL_TYPES)
        query = f"""
        MATCH (r:Record {{connectorId: $cid}})-[rel:{rel_pattern}]->(target)
        RETURN r.id AS recordId, type(rel) AS relType, target.id AS targetId, 
               properties(rel) AS props
        """
        results = await self.client.execute_query(
            query,
            {"cid": self.connector_id}
        )
        self.record_metadata = [
            {
                "recordId": r["recordId"],
                "relType": r["relType"],
                "targetId": r["targetId"],
                "props": r["props"] or {}
            }
            for r in results
        ]
        self.logger.info(f"Fetched {len(self.record_metadata)} metadata edges")

    def _create_id_mappings(self, copy_num: int) -> Dict[str, Dict[str, str]]:
        """
        Create mappings from old IDs to new IDs for a copy.
        
        Args:
            copy_num: The copy number (1, 2, 3, ...)
            
        Returns:
            Dictionary with mappings for each node type
        """
        mappings = {
            "record_groups": {},
            "records": {},
            "type_nodes": {}
        }
        
        # Map RecordGroup IDs
        for rg in self.record_groups:
            old_id = rg.get("id")
            new_id = str(uuid.uuid4())
            mappings["record_groups"][old_id] = new_id
        
        # Map Record IDs
        for record in self.records:
            old_id = record.get("id")
            new_id = str(uuid.uuid4())
            mappings["records"][old_id] = new_id
        
        # Map Type Node IDs
        for label, nodes in self.type_nodes.items():
            for node in nodes:
                old_id = node.get("id")
                new_id = str(uuid.uuid4())
                mappings["type_nodes"][old_id] = new_id
        
        return mappings

    async def _duplicate_nodes(self, copy_num: int, mappings: Dict) -> None:
        """Duplicate all nodes for a copy."""
        await self._duplicate_record_groups(copy_num, mappings)
        await self._duplicate_records(copy_num, mappings)
        await self._duplicate_type_nodes(copy_num, mappings)

    async def _duplicate_record_groups(self, copy_num: int, mappings: Dict) -> None:
        """Create duplicate RecordGroup nodes."""
        if not self.record_groups:
            return
            
        self.logger.info(f"Creating {len(self.record_groups)} RecordGroup copies...")
        
        for i in range(0, len(self.record_groups), self.batch_size):
            batch = self.record_groups[i:i + self.batch_size]
            nodes_data = []
            
            for rg in batch:
                old_id = rg.get("id")
                new_id = mappings["record_groups"][old_id]
                
                # Copy all properties
                new_node = dict(rg)
                new_node["id"] = new_id
                
                # Modify external ID to be unique
                if new_node.get("externalGroupId"):
                    new_node["externalGroupId"] = f"{new_node['externalGroupId']}_copy{copy_num}"
                
                # Remap parent reference if exists
                if new_node.get("parentRecordGroupId"):
                    old_parent = new_node["parentRecordGroupId"]
                    if old_parent in mappings["record_groups"]:
                        new_node["parentRecordGroupId"] = mappings["record_groups"][old_parent]
                
                # Add copy identification
                new_node["_isCopy"] = True
                new_node["_copyNumber"] = copy_num
                new_node["_sourceId"] = old_id
                
                nodes_data.append(new_node)
            
            await self._batch_create_nodes("RecordGroup", nodes_data)
            self.stats["record_groups_created"] += len(batch)

    async def _duplicate_records(self, copy_num: int, mappings: Dict) -> None:
        """Create duplicate Record nodes."""
        if not self.records:
            return
            
        self.logger.info(f"Creating {len(self.records)} Record copies...")
        
        for i in range(0, len(self.records), self.batch_size):
            batch = self.records[i:i + self.batch_size]
            nodes_data = []
            
            for record in batch:
                old_id = record.get("id")
                new_id = mappings["records"][old_id]
                
                # Copy all properties
                new_node = dict(record)
                new_node["id"] = new_id
                
                # Modify external ID to be unique
                if new_node.get("externalRecordId"):
                    new_node["externalRecordId"] = f"{new_node['externalRecordId']}_copy{copy_num}"
                
                # Modify external revision ID if present
                if new_node.get("externalRevisionId"):
                    new_node["externalRevisionId"] = f"{new_node['externalRevisionId']}_copy{copy_num}"
                
                # Modify external parent ID if present
                if new_node.get("externalParentId"):
                    new_node["externalParentId"] = f"{new_node['externalParentId']}_copy{copy_num}"
                
                # Modify external group ID if present
                if new_node.get("externalGroupId"):
                    new_node["externalGroupId"] = f"{new_node['externalGroupId']}_copy{copy_num}"
                
                # Remap recordGroupId if exists
                if new_node.get("recordGroupId"):
                    old_rg_id = new_node["recordGroupId"]
                    if old_rg_id in mappings["record_groups"]:
                        new_node["recordGroupId"] = mappings["record_groups"][old_rg_id]
                
                # Remap parentNodeId if exists
                if new_node.get("parentNodeId"):
                    old_parent = new_node["parentNodeId"]
                    if old_parent in mappings["records"]:
                        new_node["parentNodeId"] = mappings["records"][old_parent]
                
                # Keep virtualRecordId as-is (same as original)
                # new_node["virtualRecordId"] stays unchanged
                
                # Add copy identification
                new_node["_isCopy"] = True
                new_node["_copyNumber"] = copy_num
                new_node["_sourceId"] = old_id
                
                nodes_data.append(new_node)
            
            await self._batch_create_nodes("Record", nodes_data)
            self.stats["records_created"] += len(batch)

    async def _duplicate_type_nodes(self, copy_num: int, mappings: Dict) -> None:
        """Create duplicate type-specific nodes (File, Mail, etc.)."""
        for label, nodes in self.type_nodes.items():
            if not nodes:
                continue
                
            self.logger.info(f"Creating {len(nodes)} {label} copies...")
            
            for i in range(0, len(nodes), self.batch_size):
                batch = nodes[i:i + self.batch_size]
                nodes_data = []
                
                for node in batch:
                    old_id = node.get("id")
                    new_id = mappings["type_nodes"][old_id]
                    
                    # Copy all properties
                    new_node = dict(node)
                    new_node["id"] = new_id
                    
                    # Add copy identification
                    new_node["_isCopy"] = True
                    new_node["_copyNumber"] = copy_num
                    new_node["_sourceId"] = old_id
                    
                    nodes_data.append(new_node)
                
                await self._batch_create_nodes(label, nodes_data)
                self.stats["type_nodes_created"] += len(batch)

    async def _batch_create_nodes(self, label: str, nodes: List[Dict]) -> None:
        """Create nodes in batch."""
        if not nodes:
            return
        
        query = f"""
        UNWIND $nodes AS node
        CREATE (n:{label})
        SET n = node
        """
        await self.client.execute_query(query, {"nodes": nodes})

    async def _duplicate_relationships(self, copy_num: int, mappings: Dict) -> None:
        """Duplicate all relationships for a copy."""
        await self._duplicate_rg_hierarchy(mappings)
        await self._duplicate_rg_belongs_to_app(mappings)
        await self._duplicate_record_belongs_to(mappings)
        await self._duplicate_is_of_type(mappings)
        await self._duplicate_record_relations(mappings)
        await self._duplicate_inherit_permissions(mappings)
        await self._duplicate_rg_permissions(mappings)
        await self._duplicate_metadata_edges(mappings)

    async def _duplicate_rg_hierarchy(self, mappings: Dict) -> None:
        """Duplicate RecordGroup -> RecordGroup BELONGS_TO edges."""
        if not self.rg_belongs_to_rg:
            return
            
        self.logger.info(f"Creating {len(self.rg_belongs_to_rg)} RecordGroup hierarchy edges...")
        
        edges = []
        for edge in self.rg_belongs_to_rg:
            new_child_id = mappings["record_groups"].get(edge["childId"])
            new_parent_id = mappings["record_groups"].get(edge["parentId"])
            if new_child_id and new_parent_id:
                edges.append({
                    "fromId": new_child_id,
                    "toId": new_parent_id,
                    "props": edge["props"]
                })
        
        await self._batch_create_edges("RecordGroup", "RecordGroup", "BELONGS_TO", edges)
        self.stats["relationships_created"] += len(edges)

    async def _duplicate_rg_belongs_to_app(self, mappings: Dict) -> None:
        """Duplicate RecordGroup -> App BELONGS_TO edges (so duplicated root RGs appear in Knowledge Hub)."""
        if not self.rg_belongs_to_app:
            return

        self.logger.info(f"Creating {len(self.rg_belongs_to_app)} RecordGroup->App edges...")

        edges = []
        for edge in self.rg_belongs_to_app:
            new_rg_id = mappings["record_groups"].get(edge["rgId"])
            if new_rg_id:
                edges.append({
                    "fromId": new_rg_id,
                    "toId": edge["appId"],
                    "props": edge["props"]
                })

        if edges:
            await self._batch_create_edges("RecordGroup", "App", "BELONGS_TO", edges)
            self.stats["relationships_created"] += len(edges)

    async def _duplicate_record_belongs_to(self, mappings: Dict) -> None:
        """Duplicate Record -> RecordGroup BELONGS_TO edges."""
        if not self.record_belongs_to_rg:
            return
            
        self.logger.info(f"Creating {len(self.record_belongs_to_rg)} Record->RecordGroup edges...")
        
        edges = []
        for edge in self.record_belongs_to_rg:
            new_record_id = mappings["records"].get(edge["recordId"])
            new_rg_id = mappings["record_groups"].get(edge["rgId"])
            if new_record_id and new_rg_id:
                edges.append({
                    "fromId": new_record_id,
                    "toId": new_rg_id,
                    "props": edge["props"]
                })
        
        await self._batch_create_edges("Record", "RecordGroup", "BELONGS_TO", edges)
        self.stats["relationships_created"] += len(edges)

    async def _duplicate_is_of_type(self, mappings: Dict) -> None:
        """Duplicate Record -> TypeNode IS_OF_TYPE edges."""
        if not self.record_is_of_type:
            return
            
        self.logger.info(f"Creating {len(self.record_is_of_type)} IS_OF_TYPE edges...")
        
        # Group by type label for efficient creation
        edges_by_label: Dict[str, List[Dict]] = {}
        for edge in self.record_is_of_type:
            new_record_id = mappings["records"].get(edge["recordId"])
            new_type_id = mappings["type_nodes"].get(edge["typeId"])
            if new_record_id and new_type_id:
                label = edge["typeLabel"]
                if label not in edges_by_label:
                    edges_by_label[label] = []
                edges_by_label[label].append({
                    "fromId": new_record_id,
                    "toId": new_type_id,
                    "props": edge["props"]
                })
        
        for label, edges in edges_by_label.items():
            await self._batch_create_edges("Record", label, "IS_OF_TYPE", edges)
            self.stats["relationships_created"] += len(edges)

    async def _duplicate_record_relations(self, mappings: Dict) -> None:
        """Duplicate Record -> Record RECORD_RELATION edges."""
        if not self.record_relations:
            return
            
        self.logger.info(f"Creating {len(self.record_relations)} RECORD_RELATION edges...")
        
        edges = []
        for edge in self.record_relations:
            new_from_id = mappings["records"].get(edge["fromId"])
            new_to_id = mappings["records"].get(edge["toId"])
            if new_from_id and new_to_id:
                edges.append({
                    "fromId": new_from_id,
                    "toId": new_to_id,
                    "props": edge["props"]
                })
        
        await self._batch_create_edges("Record", "Record", "RECORD_RELATION", edges)
        self.stats["relationships_created"] += len(edges)

    async def _duplicate_inherit_permissions(self, mappings: Dict) -> None:
        """Duplicate Record -> RecordGroup INHERIT_PERMISSIONS edges."""
        if not self.record_inherit_perms:
            return
            
        self.logger.info(f"Creating {len(self.record_inherit_perms)} INHERIT_PERMISSIONS edges...")
        
        edges = []
        for edge in self.record_inherit_perms:
            new_record_id = mappings["records"].get(edge["recordId"])
            new_rg_id = mappings["record_groups"].get(edge["rgId"])
            if new_record_id and new_rg_id:
                edges.append({
                    "fromId": new_record_id,
                    "toId": new_rg_id,
                    "props": edge["props"]
                })
        
        await self._batch_create_edges("Record", "RecordGroup", "INHERIT_PERMISSIONS", edges)
        self.stats["relationships_created"] += len(edges)

    async def _duplicate_rg_permissions(self, mappings: Dict) -> None:
        """Duplicate permission edges to new RecordGroups (keep original sources)."""
        if not self.rg_permissions:
            return
            
        self.logger.info(f"Creating {len(self.rg_permissions)} PERMISSION edges to new RecordGroups...")
        
        for i in range(0, len(self.rg_permissions), self.batch_size):
            batch = self.rg_permissions[i:i + self.batch_size]
            
            for edge in batch:
                new_rg_id = mappings["record_groups"].get(edge["rgId"])
                if not new_rg_id:
                    continue
                
                source_labels = edge["sourceLabels"]
                source_id = edge["sourceId"]
                props = edge["props"]
                
                # Determine source label (prefer User, Group, Role, Organization)
                source_label = None
                for label in ["User", "Group", "Role", "Organization"]:
                    if label in source_labels:
                        source_label = label
                        break
                
                if not source_label:
                    source_label = source_labels[0] if source_labels else "Node"
                
                query = f"""
                MATCH (source:{source_label} {{id: $source_id}})
                MATCH (target:RecordGroup {{id: $target_id}})
                CREATE (source)-[r:PERMISSION]->(target)
                SET r = $props
                """
                
                await self.client.execute_query(query, {
                    "source_id": source_id,
                    "target_id": new_rg_id,
                    "props": props
                })
            
            self.stats["relationships_created"] += len(batch)

    async def _duplicate_metadata_edges(self, mappings: Dict) -> None:
        """Duplicate metadata edges (department, category, language, topic) to new Records."""
        if not self.record_metadata:
            return
            
        self.logger.info(f"Creating {len(self.record_metadata)} metadata edges...")
        
        # Group by relationship type
        edges_by_type: Dict[str, List[Dict]] = {}
        for edge in self.record_metadata:
            rel_type = edge["relType"]
            if rel_type not in edges_by_type:
                edges_by_type[rel_type] = []
            edges_by_type[rel_type].append(edge)
        
        for rel_type, edges in edges_by_type.items():
            for i in range(0, len(edges), self.batch_size):
                batch = edges[i:i + self.batch_size]
                edges_data = []
                
                for edge in batch:
                    new_record_id = mappings["records"].get(edge["recordId"])
                    if new_record_id:
                        edges_data.append({
                            "fromId": new_record_id,
                            "toId": edge["targetId"],
                            "props": edge["props"]
                        })
                
                if edges_data:
                    # Create edges - target node keeps original ID (Department, Category, etc.)
                    query = f"""
                    UNWIND $edges AS edge
                    MATCH (r:Record {{id: edge.fromId}})
                    MATCH (t {{id: edge.toId}})
                    CREATE (r)-[rel:{rel_type}]->(t)
                    SET rel = edge.props
                    """
                    await self.client.execute_query(query, {"edges": edges_data})
                    self.stats["relationships_created"] += len(edges_data)

    async def _batch_create_edges(
        self, 
        from_label: str, 
        to_label: str, 
        rel_type: str, 
        edges: List[Dict]
    ) -> None:
        """Create relationships in batch."""
        if not edges:
            return
        
        for i in range(0, len(edges), self.batch_size):
            batch = edges[i:i + self.batch_size]
            
            query = f"""
            UNWIND $edges AS edge
            MATCH (from:{from_label} {{id: edge.fromId}})
            MATCH (to:{to_label} {{id: edge.toId}})
            CREATE (from)-[r:{rel_type}]->(to)
            SET r = edge.props
            """
            await self.client.execute_query(query, {"edges": batch})

    def _print_original_data_summary(self) -> None:
        """Print summary of original data fetched."""
        total_type_nodes = sum(len(nodes) for nodes in self.type_nodes.values())
        total_edges = (
            len(self.rg_belongs_to_rg) +
            len(self.rg_belongs_to_app) +
            len(self.record_belongs_to_rg) +
            len(self.record_is_of_type) +
            len(self.record_relations) +
            len(self.record_inherit_perms) +
            len(self.rg_permissions) +
            len(self.record_metadata)
        )
        
        self.logger.info("\n--- Original Data Summary ---")
        self.logger.info(f"RecordGroups: {len(self.record_groups)}")
        self.logger.info(f"Records: {len(self.records)}")
        self.logger.info(f"Type nodes: {total_type_nodes}")
        for label, nodes in self.type_nodes.items():
            self.logger.info(f"  - {label}: {len(nodes)}")
        self.logger.info(f"Total relationships: {total_edges}")
        self.logger.info(f"  - RG hierarchy: {len(self.rg_belongs_to_rg)}")
        self.logger.info(f"  - RG->App: {len(self.rg_belongs_to_app)}")
        self.logger.info(f"  - Record->RG: {len(self.record_belongs_to_rg)}")
        self.logger.info(f"  - IS_OF_TYPE: {len(self.record_is_of_type)}")
        self.logger.info(f"  - RECORD_RELATION: {len(self.record_relations)}")
        self.logger.info(f"  - INHERIT_PERMISSIONS: {len(self.record_inherit_perms)}")
        self.logger.info(f"  - RG PERMISSIONS: {len(self.rg_permissions)}")
        self.logger.info(f"  - Metadata edges: {len(self.record_metadata)}")

    def _print_dry_run_projection(self) -> None:
        """Print projected counts for dry run."""
        total_type_nodes = sum(len(nodes) for nodes in self.type_nodes.values())
        total_edges = (
            len(self.rg_belongs_to_rg) +
            len(self.rg_belongs_to_app) +
            len(self.record_belongs_to_rg) +
            len(self.record_is_of_type) +
            len(self.record_relations) +
            len(self.record_inherit_perms) +
            len(self.rg_permissions) +
            len(self.record_metadata)
        )
        
        self.logger.info("\n--- Dry Run Projection ---")
        self.logger.info(f"Would create {self.copies} copies of:")
        self.logger.info(f"  RecordGroups: {len(self.record_groups)} x {self.copies} = {len(self.record_groups) * self.copies}")
        self.logger.info(f"  Records: {len(self.records)} x {self.copies} = {len(self.records) * self.copies}")
        self.logger.info(f"  Type nodes: {total_type_nodes} x {self.copies} = {total_type_nodes * self.copies}")
        self.logger.info(f"  Relationships: {total_edges} x {self.copies} = {total_edges * self.copies}")
        self.logger.info("\nNo data was created (dry run mode)")

    def _print_final_statistics(self) -> None:
        """Print final creation statistics."""
        self.logger.info("\n--- Final Statistics ---")
        self.logger.info(f"RecordGroups created: {self.stats['record_groups_created']}")
        self.logger.info(f"Records created: {self.stats['records_created']}")
        self.logger.info(f"Type nodes created: {self.stats['type_nodes_created']}")
        self.logger.info(f"Relationships created: {self.stats['relationships_created']}")
        total = (
            self.stats['record_groups_created'] +
            self.stats['records_created'] +
            self.stats['type_nodes_created'] +
            self.stats['relationships_created']
        )
        self.logger.info(f"Total entities created: {total}")


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Duplicate connector data in Neo4j for load testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Preview what would be created (dry run)
    python -m scripts.duplicate_connector_data --connector-id abc123 --copies 5 --dry-run
    
    # Create 10 copies of connector data
    python -m scripts.duplicate_connector_data --connector-id abc123 --copies 10
    
    # Create copies with custom batch size
    python -m scripts.duplicate_connector_data --connector-id abc123 --copies 10 --batch-size 1000
        """
    )
    
    parser.add_argument(
        "--connector-id",
        required=True,
        help="The connector ID to duplicate data for"
    )
    parser.add_argument(
        "--copies",
        type=int,
        required=True,
        help="Number of duplicate copies to create (1-100)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Batch size for database operations (default: 500)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview counts without creating data"
    )
    
    args = parser.parse_args()
    
    # Validate copies
    if args.copies < 1 or args.copies > 100:
        parser.error("--copies must be between 1 and 100")
    
    # Setup logging
    logger = setup_logging()
    
    # Get Neo4j connection details from environment
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    username = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "")
    database = os.getenv("NEO4J_DATABASE", "neo4j")
    
    if not password:
        logger.error("NEO4J_PASSWORD environment variable is required")
        sys.exit(1)
    
    # Create Neo4j client
    client = Neo4jClient(
        uri=uri,
        username=username,
        password=password,
        database=database,
        logger=logger
    )
    
    try:
        # Connect to Neo4j
        if not await client.connect():
            logger.error("Failed to connect to Neo4j")
            sys.exit(1)
        
        # Create duplicator and run
        duplicator = ConnectorDataDuplicator(
            client=client,
            connector_id=args.connector_id,
            copies=args.copies,
            batch_size=args.batch_size,
            logger=logger
        )
        
        await duplicator.run(dry_run=args.dry_run)
        
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
