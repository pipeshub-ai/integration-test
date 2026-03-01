#!/usr/bin/env python3
"""
ArangoDB Connector Data Duplication Script

Duplicates RecordGroups, Records, type-specific nodes, and their relationships
for a given connector to enable load testing with larger datasets.

Usage:
    python -m scripts.duplicate_connector_data_arango --connector-id <id> --copies <n>
    python -m scripts.duplicate_connector_data_arango --connector-id <id> --copies <n> --dry-run
"""

import argparse
import asyncio
import logging
import os
import sys
import time
import uuid
from typing import Any, Dict, List, Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.config.constants.arangodb import CollectionNames
from app.services.graph_db.arango.arango_http_client import ArangoHTTPClient


def setup_logging() -> logging.Logger:
    """Configure logging for the script."""
    logger = logging.getLogger("duplicate_connector_data_arango")
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


class ConnectorDataDuplicatorArango:
    """Duplicates connector data in ArangoDB for load testing."""

    # Type-specific collection names
    TYPE_COLLECTIONS = [
        CollectionNames.FILES.value,
        CollectionNames.MAILS.value,
        CollectionNames.WEBPAGES.value,
        CollectionNames.COMMENTS.value,
        CollectionNames.TICKETS.value,
        CollectionNames.LINKS.value,
        CollectionNames.PROJECTS.value,
    ]
    
    # Metadata edge collection names
    METADATA_EDGE_COLLECTIONS = [
        CollectionNames.BELONGS_TO_DEPARTMENT.value,
        CollectionNames.BELONGS_TO_CATEGORY.value,
        CollectionNames.BELONGS_TO_LANGUAGE.value,
        CollectionNames.BELONGS_TO_TOPIC.value,
    ]

    def __init__(
        self,
        client: ArangoHTTPClient,
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
        self.type_nodes: Dict[str, List[Dict]] = {}  # collection -> nodes
        
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
        
        self.logger.info(f"Starting ArangoDB data duplication for connector: {self.connector_id}")
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
        FOR rg IN @@collection
            FILTER rg.connectorId == @connector_id
            RETURN rg
        """
        results = await self.client.execute_aql(
            query,
            {
                "@collection": CollectionNames.RECORD_GROUPS.value,
                "connector_id": self.connector_id
            }
        )
        self.record_groups = results
        self.logger.info(f"Fetched {len(self.record_groups)} RecordGroups")

    async def _fetch_records(self) -> None:
        """Fetch all Records for the connector."""
        query = """
        FOR r IN @@collection
            FILTER r.connectorId == @connector_id
            RETURN r
        """
        results = await self.client.execute_aql(
            query,
            {
                "@collection": CollectionNames.RECORDS.value,
                "connector_id": self.connector_id
            }
        )
        self.records = results
        self.logger.info(f"Fetched {len(self.records)} Records")

    async def _fetch_type_nodes(self) -> None:
        """Fetch all type-specific nodes connected to the connector's records."""
        for collection in self.TYPE_COLLECTIONS:
            query = """
            FOR r IN @@records
                FILTER r.connectorId == @connector_id
                FOR edge IN @@is_of_type
                    FILTER edge._from == CONCAT("records/", r._key)
                    LET type_node = DOCUMENT(edge._to)
                    FILTER type_node != null
                    FILTER PARSE_IDENTIFIER(edge._to).collection == @type_collection
                    RETURN DISTINCT type_node
            """
            results = await self.client.execute_aql(
                query,
                {
                    "@records": CollectionNames.RECORDS.value,
                    "@is_of_type": CollectionNames.IS_OF_TYPE.value,
                    "connector_id": self.connector_id,
                    "type_collection": collection
                }
            )
            if results:
                self.type_nodes[collection] = results
                self.logger.info(f"Fetched {len(results)} {collection} nodes")

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
        FOR child IN @@record_groups
            FILTER child.connectorId == @cid
            FOR edge IN @@belongs_to
                FILTER edge._from == CONCAT("recordGroups/", child._key)
                LET parent_id = PARSE_IDENTIFIER(edge._to)
                FILTER parent_id.collection == "recordGroups"
                LET parent = DOCUMENT(edge._to)
                FILTER parent != null AND parent.connectorId == @cid
                RETURN {
                    childKey: child._key,
                    parentKey: parent._key,
                    edge: edge
                }
        """
        results = await self.client.execute_aql(
            query,
            {
                "@record_groups": CollectionNames.RECORD_GROUPS.value,
                "@belongs_to": CollectionNames.BELONGS_TO.value,
                "cid": self.connector_id
            }
        )
        self.rg_belongs_to_rg = results
        self.logger.info(f"Fetched {len(self.rg_belongs_to_rg)} RecordGroup hierarchy edges")

    async def _fetch_rg_belongs_to_app(self) -> None:
        """Fetch RecordGroup -> App BELONGS_TO relationships (root RecordGroups)."""
        query = """
        FOR rg IN @@record_groups
            FILTER rg.connectorId == @cid
            FOR edge IN @@belongs_to
                FILTER edge._from == CONCAT("recordGroups/", rg._key)
                LET target_id = PARSE_IDENTIFIER(edge._to)
                FILTER target_id.collection == "apps"
                RETURN {
                    rgKey: rg._key,
                    appKey: target_id.key,
                    edge: edge
                }
        """
        results = await self.client.execute_aql(
            query,
            {
                "@record_groups": CollectionNames.RECORD_GROUPS.value,
                "@belongs_to": CollectionNames.BELONGS_TO.value,
                "cid": self.connector_id
            }
        )
        self.rg_belongs_to_app = results
        self.logger.info(f"Fetched {len(self.rg_belongs_to_app)} RecordGroup->App edges")

    async def _fetch_record_belongs_to(self) -> None:
        """Fetch Record -> RecordGroup BELONGS_TO relationships."""
        query = """
        FOR r IN @@records
            FILTER r.connectorId == @cid
            FOR edge IN @@belongs_to
                FILTER edge._from == CONCAT("records/", r._key)
                LET target_id = PARSE_IDENTIFIER(edge._to)
                FILTER target_id.collection == "recordGroups"
                LET rg = DOCUMENT(edge._to)
                FILTER rg != null AND rg.connectorId == @cid
                RETURN {
                    recordKey: r._key,
                    rgKey: rg._key,
                    edge: edge
                }
        """
        results = await self.client.execute_aql(
            query,
            {
                "@records": CollectionNames.RECORDS.value,
                "@belongs_to": CollectionNames.BELONGS_TO.value,
                "cid": self.connector_id
            }
        )
        self.record_belongs_to_rg = results
        self.logger.info(f"Fetched {len(self.record_belongs_to_rg)} Record->RecordGroup edges")

    async def _fetch_is_of_type(self) -> None:
        """Fetch Record -> TypeNode IS_OF_TYPE relationships."""
        query = """
        FOR r IN @@records
            FILTER r.connectorId == @cid
            FOR edge IN @@is_of_type
                FILTER edge._from == CONCAT("records/", r._key)
                LET target_id = PARSE_IDENTIFIER(edge._to)
                FILTER target_id.collection IN @type_collections
                RETURN {
                    recordKey: r._key,
                    typeKey: target_id.key,
                    typeCollection: target_id.collection,
                    edge: edge
                }
        """
        results = await self.client.execute_aql(
            query,
            {
                "@records": CollectionNames.RECORDS.value,
                "@is_of_type": CollectionNames.IS_OF_TYPE.value,
                "cid": self.connector_id,
                "type_collections": self.TYPE_COLLECTIONS
            }
        )
        self.record_is_of_type = results
        self.logger.info(f"Fetched {len(self.record_is_of_type)} IS_OF_TYPE edges")

    async def _fetch_record_relations(self) -> None:
        """Fetch Record -> Record RECORD_RELATION relationships."""
        query = """
        FOR r1 IN @@records
            FILTER r1.connectorId == @cid
            FOR edge IN @@record_relations
                FILTER edge._from == CONCAT("records/", r1._key)
                LET target_id = PARSE_IDENTIFIER(edge._to)
                FILTER target_id.collection == "records"
                LET r2 = DOCUMENT(edge._to)
                FILTER r2 != null AND r2.connectorId == @cid
                RETURN {
                    fromKey: r1._key,
                    toKey: r2._key,
                    edge: edge
                }
        """
        results = await self.client.execute_aql(
            query,
            {
                "@records": CollectionNames.RECORDS.value,
                "@record_relations": CollectionNames.RECORD_RELATIONS.value,
                "cid": self.connector_id
            }
        )
        self.record_relations = results
        self.logger.info(f"Fetched {len(self.record_relations)} RECORD_RELATION edges")

    async def _fetch_inherit_permissions(self) -> None:
        """Fetch Record -> RecordGroup INHERIT_PERMISSIONS relationships."""
        query = """
        FOR r IN @@records
            FILTER r.connectorId == @cid
            FOR edge IN @@inherit_permissions
                FILTER edge._from == CONCAT("records/", r._key)
                LET target_id = PARSE_IDENTIFIER(edge._to)
                FILTER target_id.collection == "recordGroups"
                LET rg = DOCUMENT(edge._to)
                FILTER rg != null AND rg.connectorId == @cid
                RETURN {
                    recordKey: r._key,
                    rgKey: rg._key,
                    edge: edge
                }
        """
        results = await self.client.execute_aql(
            query,
            {
                "@records": CollectionNames.RECORDS.value,
                "@inherit_permissions": CollectionNames.INHERIT_PERMISSIONS.value,
                "cid": self.connector_id
            }
        )
        self.record_inherit_perms = results
        self.logger.info(f"Fetched {len(self.record_inherit_perms)} INHERIT_PERMISSIONS edges")

    async def _fetch_rg_permissions(self) -> None:
        """Fetch permission edges to RecordGroups (from Users, Groups, Roles, Organizations)."""
        query = """
        FOR rg IN @@record_groups
            FILTER rg.connectorId == @cid
            FOR edge IN @@permission
                FILTER edge._to == CONCAT("recordGroups/", rg._key)
                LET source_id = PARSE_IDENTIFIER(edge._from)
                RETURN {
                    sourceKey: source_id.key,
                    sourceCollection: source_id.collection,
                    rgKey: rg._key,
                    edge: edge
                }
        """
        results = await self.client.execute_aql(
            query,
            {
                "@record_groups": CollectionNames.RECORD_GROUPS.value,
                "@permission": CollectionNames.PERMISSION.value,
                "cid": self.connector_id
            }
        )
        self.rg_permissions = results
        self.logger.info(f"Fetched {len(self.rg_permissions)} PERMISSION edges to RecordGroups")

    async def _fetch_metadata_edges(self) -> None:
        """Fetch metadata edges from Records (department, category, language, topic)."""
        all_metadata = []
        for edge_collection in self.METADATA_EDGE_COLLECTIONS:
            query = """
            FOR r IN @@records
                FILTER r.connectorId == @cid
                FOR edge IN @@edge_collection
                    FILTER edge._from == CONCAT("records/", r._key)
                    LET target_id = PARSE_IDENTIFIER(edge._to)
                    RETURN {
                        recordKey: r._key,
                        targetKey: target_id.key,
                        targetCollection: target_id.collection,
                        edgeCollection: @edge_collection_name,
                        edge: edge
                    }
            """
            results = await self.client.execute_aql(
                query,
                {
                    "@records": CollectionNames.RECORDS.value,
                    "@edge_collection": edge_collection,
                    "edge_collection_name": edge_collection,
                    "cid": self.connector_id
                }
            )
            all_metadata.extend(results)
        
        self.record_metadata = all_metadata
        self.logger.info(f"Fetched {len(self.record_metadata)} metadata edges")

    def _create_id_mappings(self, copy_num: int) -> Dict[str, Dict[str, str]]:
        """
        Create mappings from old keys to new keys for a copy.
        
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
        
        # Map RecordGroup keys
        for rg in self.record_groups:
            old_key = rg.get("_key")
            new_key = str(uuid.uuid4())
            mappings["record_groups"][old_key] = new_key
        
        # Map Record keys
        for record in self.records:
            old_key = record.get("_key")
            new_key = str(uuid.uuid4())
            mappings["records"][old_key] = new_key
        
        # Map Type Node keys
        for collection, nodes in self.type_nodes.items():
            for node in nodes:
                old_key = node.get("_key")
                new_key = str(uuid.uuid4())
                mappings["type_nodes"][old_key] = new_key
        
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
                old_key = rg.get("_key")
                new_key = mappings["record_groups"][old_key]
                
                # Copy all properties except system fields
                new_node = {k: v for k, v in rg.items() if not k.startswith("_")}
                new_node["_key"] = new_key
                
                # Modify external ID to be unique
                if new_node.get("externalGroupId"):
                    new_node["externalGroupId"] = f"{new_node['externalGroupId']}_copy{copy_num}"
                
                # Remap parent reference if exists (by _key; schema may use id - keep only schema-allowed fields)
                if new_node.get("parentRecordGroupId"):
                    old_parent = new_node["parentRecordGroupId"]
                    if old_parent in mappings["record_groups"]:
                        new_node["parentRecordGroupId"] = mappings["record_groups"][old_parent]
                
                # Note: ArangoDB record group schema has additionalProperties: False,
                # so we cannot add _isCopy/_copyNumber/_sourceKey. Copies are identifiable
                # via externalGroupId suffix (_copyN).
                nodes_data.append(new_node)
            
            await self.client.batch_insert_documents(
                CollectionNames.RECORD_GROUPS.value,
                nodes_data,
                overwrite=False
            )
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
                old_key = record.get("_key")
                new_key = mappings["records"][old_key]
                
                # Copy all properties except system fields
                new_node = {k: v for k, v in record.items() if not k.startswith("_")}
                new_node["_key"] = new_key
                
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
                    old_rg_key = new_node["recordGroupId"]
                    if old_rg_key in mappings["record_groups"]:
                        new_node["recordGroupId"] = mappings["record_groups"][old_rg_key]
                
                # Remap parentNodeId if exists
                if new_node.get("parentNodeId"):
                    old_parent = new_node["parentNodeId"]
                    if old_parent in mappings["records"]:
                        new_node["parentNodeId"] = mappings["records"][old_parent]
                
                # Keep virtualRecordId as-is (same as original)
                
                # Note: ArangoDB record schema has additionalProperties: False,
                # so we cannot add _isCopy/_copyNumber/_sourceKey. Copies are identifiable
                # via externalRecordId suffix (_copyN).
                nodes_data.append(new_node)
            
            await self.client.batch_insert_documents(
                CollectionNames.RECORDS.value,
                nodes_data,
                overwrite=False
            )
            self.stats["records_created"] += len(batch)

    async def _duplicate_type_nodes(self, copy_num: int, mappings: Dict) -> None:
        """Create duplicate type-specific nodes (File, Mail, etc.)."""
        for collection, nodes in self.type_nodes.items():
            if not nodes:
                continue
                
            self.logger.info(f"Creating {len(nodes)} {collection} copies...")
            
            for i in range(0, len(nodes), self.batch_size):
                batch = nodes[i:i + self.batch_size]
                nodes_data = []
                
                for node in batch:
                    old_key = node.get("_key")
                    new_key = mappings["type_nodes"][old_key]
                    
                    # Copy all properties except system fields
                    new_node = {k: v for k, v in node.items() if not k.startswith("_")}
                    new_node["_key"] = new_key
                    
                    # Note: Type collection schemas have additionalProperties: False,
                    # so we cannot add _isCopy/_copyNumber/_sourceKey.
                    nodes_data.append(new_node)
                
                await self.client.batch_insert_documents(
                    collection,
                    nodes_data,
                    overwrite=False
                )
                self.stats["type_nodes_created"] += len(batch)

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
        for item in self.rg_belongs_to_rg:
            new_child_key = mappings["record_groups"].get(item["childKey"])
            new_parent_key = mappings["record_groups"].get(item["parentKey"])
            if new_child_key and new_parent_key:
                edge_props = {k: v for k, v in item["edge"].items() if not k.startswith("_")}
                edge_props["_from"] = f"{CollectionNames.RECORD_GROUPS.value}/{new_child_key}"
                edge_props["_to"] = f"{CollectionNames.RECORD_GROUPS.value}/{new_parent_key}"
                edges.append(edge_props)
        
        if edges:
            await self._batch_create_edges(CollectionNames.BELONGS_TO.value, edges)
            self.stats["relationships_created"] += len(edges)

    async def _duplicate_rg_belongs_to_app(self, mappings: Dict) -> None:
        """Duplicate RecordGroup -> App BELONGS_TO edges (so duplicated root RGs appear in Knowledge Hub)."""
        if not self.rg_belongs_to_app:
            return

        self.logger.info(f"Creating {len(self.rg_belongs_to_app)} RecordGroup->App edges...")

        edges = []
        for item in self.rg_belongs_to_app:
            new_rg_key = mappings["record_groups"].get(item["rgKey"])
            if new_rg_key:
                edge_props = {k: v for k, v in item["edge"].items() if not k.startswith("_")}
                edge_props["_from"] = f"{CollectionNames.RECORD_GROUPS.value}/{new_rg_key}"
                edge_props["_to"] = f"{CollectionNames.APPS.value}/{item['appKey']}"
                edges.append(edge_props)

        if edges:
            await self._batch_create_edges(CollectionNames.BELONGS_TO.value, edges)
            self.stats["relationships_created"] += len(edges)

    async def _duplicate_record_belongs_to(self, mappings: Dict) -> None:
        """Duplicate Record -> RecordGroup BELONGS_TO edges."""
        if not self.record_belongs_to_rg:
            return
            
        self.logger.info(f"Creating {len(self.record_belongs_to_rg)} Record->RecordGroup edges...")
        
        edges = []
        for item in self.record_belongs_to_rg:
            new_record_key = mappings["records"].get(item["recordKey"])
            new_rg_key = mappings["record_groups"].get(item["rgKey"])
            if new_record_key and new_rg_key:
                edge_props = {k: v for k, v in item["edge"].items() if not k.startswith("_")}
                edge_props["_from"] = f"{CollectionNames.RECORDS.value}/{new_record_key}"
                edge_props["_to"] = f"{CollectionNames.RECORD_GROUPS.value}/{new_rg_key}"
                edges.append(edge_props)
        
        if edges:
            await self._batch_create_edges(CollectionNames.BELONGS_TO.value, edges)
            self.stats["relationships_created"] += len(edges)

    async def _duplicate_is_of_type(self, mappings: Dict) -> None:
        """Duplicate Record -> TypeNode IS_OF_TYPE edges."""
        if not self.record_is_of_type:
            return
            
        self.logger.info(f"Creating {len(self.record_is_of_type)} IS_OF_TYPE edges...")
        
        edges = []
        for item in self.record_is_of_type:
            new_record_key = mappings["records"].get(item["recordKey"])
            new_type_key = mappings["type_nodes"].get(item["typeKey"])
            if new_record_key and new_type_key:
                edge_props = {k: v for k, v in item["edge"].items() if not k.startswith("_")}
                edge_props["_from"] = f"{CollectionNames.RECORDS.value}/{new_record_key}"
                edge_props["_to"] = f"{item['typeCollection']}/{new_type_key}"
                edges.append(edge_props)
        
        if edges:
            await self._batch_create_edges(CollectionNames.IS_OF_TYPE.value, edges)
            self.stats["relationships_created"] += len(edges)

    async def _duplicate_record_relations(self, mappings: Dict) -> None:
        """Duplicate Record -> Record RECORD_RELATION edges."""
        if not self.record_relations:
            return
            
        self.logger.info(f"Creating {len(self.record_relations)} RECORD_RELATION edges...")
        
        edges = []
        for item in self.record_relations:
            new_from_key = mappings["records"].get(item["fromKey"])
            new_to_key = mappings["records"].get(item["toKey"])
            if new_from_key and new_to_key:
                edge_props = {k: v for k, v in item["edge"].items() if not k.startswith("_")}
                edge_props["_from"] = f"{CollectionNames.RECORDS.value}/{new_from_key}"
                edge_props["_to"] = f"{CollectionNames.RECORDS.value}/{new_to_key}"
                edges.append(edge_props)
        
        if edges:
            await self._batch_create_edges(CollectionNames.RECORD_RELATIONS.value, edges)
            self.stats["relationships_created"] += len(edges)

    async def _duplicate_inherit_permissions(self, mappings: Dict) -> None:
        """Duplicate Record -> RecordGroup INHERIT_PERMISSIONS edges."""
        if not self.record_inherit_perms:
            return
            
        self.logger.info(f"Creating {len(self.record_inherit_perms)} INHERIT_PERMISSIONS edges...")
        
        edges = []
        for item in self.record_inherit_perms:
            new_record_key = mappings["records"].get(item["recordKey"])
            new_rg_key = mappings["record_groups"].get(item["rgKey"])
            if new_record_key and new_rg_key:
                edge_props = {k: v for k, v in item["edge"].items() if not k.startswith("_")}
                edge_props["_from"] = f"{CollectionNames.RECORDS.value}/{new_record_key}"
                edge_props["_to"] = f"{CollectionNames.RECORD_GROUPS.value}/{new_rg_key}"
                edges.append(edge_props)
        
        if edges:
            await self._batch_create_edges(CollectionNames.INHERIT_PERMISSIONS.value, edges)
            self.stats["relationships_created"] += len(edges)

    async def _duplicate_rg_permissions(self, mappings: Dict) -> None:
        """Duplicate permission edges to new RecordGroups (keep original sources)."""
        if not self.rg_permissions:
            return
            
        self.logger.info(f"Creating {len(self.rg_permissions)} PERMISSION edges to new RecordGroups...")
        
        edges = []
        for item in self.rg_permissions:
            new_rg_key = mappings["record_groups"].get(item["rgKey"])
            if new_rg_key:
                edge_props = {k: v for k, v in item["edge"].items() if not k.startswith("_")}
                edge_props["_from"] = f"{item['sourceCollection']}/{item['sourceKey']}"
                edge_props["_to"] = f"{CollectionNames.RECORD_GROUPS.value}/{new_rg_key}"
                edges.append(edge_props)
        
        if edges:
            await self._batch_create_edges(CollectionNames.PERMISSION.value, edges)
            self.stats["relationships_created"] += len(edges)

    async def _duplicate_metadata_edges(self, mappings: Dict) -> None:
        """Duplicate metadata edges (department, category, language, topic) to new Records."""
        if not self.record_metadata:
            return
            
        self.logger.info(f"Creating {len(self.record_metadata)} metadata edges...")
        
        # Group by edge collection
        edges_by_collection: Dict[str, List[Dict]] = {}
        for item in self.record_metadata:
            new_record_key = mappings["records"].get(item["recordKey"])
            if new_record_key:
                edge_props = {k: v for k, v in item["edge"].items() if not k.startswith("_")}
                edge_props["_from"] = f"{CollectionNames.RECORDS.value}/{new_record_key}"
                edge_props["_to"] = f"{item['targetCollection']}/{item['targetKey']}"
                
                collection = item["edgeCollection"]
                if collection not in edges_by_collection:
                    edges_by_collection[collection] = []
                edges_by_collection[collection].append(edge_props)
        
        for collection, edges in edges_by_collection.items():
            if edges:
                await self._batch_create_edges(collection, edges)
                self.stats["relationships_created"] += len(edges)

    async def _batch_create_edges(self, collection: str, edges: List[Dict]) -> None:
        """Create edges in batch using AQL UPSERT."""
        if not edges:
            return
        
        for i in range(0, len(edges), self.batch_size):
            batch = edges[i:i + self.batch_size]
            
            query = """
            FOR edge IN @edges
                UPSERT { _from: edge._from, _to: edge._to }
                INSERT edge
                UPDATE edge
                IN @@collection
                RETURN NEW
            """
            await self.client.execute_aql(
                query,
                {"edges": batch, "@collection": collection}
            )

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
        for collection, nodes in self.type_nodes.items():
            self.logger.info(f"  - {collection}: {len(nodes)}")
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
        description="Duplicate connector data in ArangoDB for load testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Preview what would be created (dry run)
    python -m scripts.duplicate_connector_data_arango --connector-id abc123 --copies 5 --dry-run
    
    # Create 10 copies of connector data
    python -m scripts.duplicate_connector_data_arango --connector-id abc123 --copies 10
    
    # Create copies with custom batch size
    python -m scripts.duplicate_connector_data_arango --connector-id abc123 --copies 10 --batch-size 1000
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
    
    # Get ArangoDB connection details from environment
    base_url = os.getenv("ARANGO_URL", "http://localhost:8529")
    username = os.getenv("ARANGO_USERNAME", "root")
    password = os.getenv("ARANGO_PASSWORD", "")
    database = os.getenv("ARANGO_DB_NAME", "pipeshub")
    
    if not password:
        logger.error("ARANGO_PASSWORD environment variable is required")
        sys.exit(1)
    
    # Create ArangoDB client
    client = ArangoHTTPClient(
        base_url=base_url,
        username=username,
        password=password,
        database=database,
        logger=logger
    )
    
    try:
        # Connect to ArangoDB
        if not await client.connect():
            logger.error("Failed to connect to ArangoDB")
            sys.exit(1)
        
        # Create duplicator and run
        duplicator = ConnectorDataDuplicatorArango(
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
