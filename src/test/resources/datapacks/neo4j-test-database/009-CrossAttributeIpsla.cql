// Adds a schema-level CrossAttribute relationship type "ipsla" from neType:cpe to itself, plus 2 instance-level
// CrossAttribute relationships of that type between 2 distinct pairs of existing cpes from
// 007-NetworkElements_LIVINGOBJECTS.cql. Used by ExportExtensionTest's cross-attribute relationship scenario.
MATCH (netCpe:Attribute {_type:'neType', name:'cpe'})
CREATE (netCpe)-[:CrossAttribute {_type:'ipsla'}]->(netCpe)
;

MATCH (cpe1:NetworkElement {tag:'class=neType,cpe=CC_RJ45,neType=cpe'})
MATCH (cpe2:NetworkElement {tag:'class=neType,cpe=HW_3615,neType=cpe'})
MATCH (cpe3:NetworkElement {tag:'class=neType,cpe=AA_RJ45,neType=cpe'})
MATCH (cpe4:NetworkElement {tag:'class=neType,cpe=BB_3615,neType=cpe'})
CREATE (cpe1)-[:CrossAttribute {_type:'ipsla', latencyMs:15}]->(cpe2)
CREATE (cpe3)-[:CrossAttribute {_type:'ipsla', latencyMs:99}]->(cpe4)
;
