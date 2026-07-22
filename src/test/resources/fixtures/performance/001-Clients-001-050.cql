// Performance fixture: 1000 independent clients, each with its own topology (1000
// areas, 1000 networks, 1000 sites, 1000 cpes, 1000 wanLinks,
// 1000 viewpoints per client), reusing existing Attribute types/cardinalities from
// 002-scope_attributes.cql (no new Attribute nodes are created here). This mirrors production, where the graph is
// a forest of many separate per-client topologies rather than one giant shared one.
// Respects: cpe->site (1..1), site->client (1..1), wanLink->cpe (1..1), viewpoint->cpe (1..1),
// and the two multi-parent (0..n) cardinalities, each capped at 3 parents per child:
//   site->area (0..n) and wanLink->cluster:network (0..n).
// Batched into 20 files of 50 clients each so every file/transaction only holds one
// batch's worth of data in memory at a time (see WithNeo4jImpermanentDatabase: one transaction per file).

UNWIND range(1, 50) AS c
CREATE (client:Element:NetworkElement:Scope {tag:'class=cluster,cluster=client,client=perf' + c, name:'PerfClient' + c, _type:'cluster:client', id:'perf' + c, createdBy:'system', createdAt:1446047124000})
WITH c, client
UNWIND range(1, 800) AS ai
CREATE (a:Element:NetworkElement {tag:'class=cluster,cluster=area,area=perf' + c + 'area' + ai, name:'PerfArea' + c + '_' + ai, _type:'cluster:area', createdBy:'system', createdAt:1446047124000})
CREATE (client)<-[:Connect]-(a)
WITH DISTINCT c, client
UNWIND range(1, 800) AS ni
CREATE (n:Element:NetworkElement {tag:'class=cluster,cluster=network,network=perf' + c + 'net' + ni, name:'PerfNetwork' + c + '_' + ni, _type:'cluster:network', createdBy:'system', createdAt:1446047124000})
CREATE (client)<-[:Connect]-(n)
;

UNWIND range(1, 50) AS c
UNWIND range(1, 800) AS s
MATCH (client:NetworkElement {tag:'class=cluster,cluster=client,client=perf' + c})
CREATE (site:Element:NetworkElement {tag:'class=cluster,cluster=site,site=perf' + c + '_' + s, name:'PerfSite' + c + '_' + s, _type:'cluster:site', createdBy:'system', createdAt:1446047124000})
CREATE (client)<-[:Connect]-(site)
CREATE (cpe:Element:NetworkElement {tag:'class=neType,neType=cpe,cpe=perf' + c + '_' + s, name:'PerfCpe' + c + '_' + s, _type:'neType:cpe', ip:'10.0.0.1', createdBy:'system', createdAt:1446047124000})
CREATE (site)<-[:Connect]-(cpe)
CREATE (wl:Element:NetworkElement {tag:'class=neType,neType=wanLink,wanLink=perf' + c + '_' + s, name:'PerfWanLink' + c + '_' + s, _type:'neType:wanLink', pollingIfIndex:1, createdAt:1446047124000, bandwidthIn:10000, bandwidthOut:10000})
CREATE (cpe)<-[:Connect]-(wl)
CREATE (vp:Element:NetworkElement {tag:'class=neType,neType=viewpoint,viewpoint=perf' + c + '_' + s, name:'PerfViewpoint' + c + '_' + s, _type:'neType:viewpoint', ifIndexes:[1], createdAt:1446047124000})
CREATE (cpe)<-[:Connect]-(vp)
;

UNWIND range(1, 50) AS c
UNWIND range(1, 800) AS s
MATCH (site:NetworkElement {tag:'class=cluster,cluster=site,site=perf' + c + '_' + s})
UNWIND range(0, s % 3) AS off
MATCH (a:NetworkElement {tag:'class=cluster,cluster=area,area=perf' + c + 'area' + (((s + off - 1) % 1000) + 1)})
CREATE (a)<-[:Connect]-(site)
;

UNWIND range(1, 50) AS c
UNWIND range(1, 800) AS s
MATCH (wl:NetworkElement {tag:'class=neType,neType=wanLink,wanLink=perf' + c + '_' + s})
UNWIND range(0, (s + 1) % 3) AS off
MATCH (n:NetworkElement {tag:'class=cluster,cluster=network,network=perf' + c + 'net' + (((s + off) % 1000) + 1)})
CREATE (n)<-[:Connect]-(wl)
;
