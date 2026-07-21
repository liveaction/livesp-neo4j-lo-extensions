UNWIND range(51, 100) AS c
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

UNWIND range(51, 100) AS c
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

UNWIND range(51, 100) AS c
UNWIND range(1, 800) AS s
MATCH (site:NetworkElement {tag:'class=cluster,cluster=site,site=perf' + c + '_' + s})
UNWIND range(0, s % 3) AS off
MATCH (a:NetworkElement {tag:'class=cluster,cluster=area,area=perf' + c + 'area' + (((s + off - 1) % 1000) + 1)})
CREATE (a)<-[:Connect]-(site)
;

UNWIND range(51, 100) AS c
UNWIND range(1, 800) AS s
MATCH (wl:NetworkElement {tag:'class=neType,neType=wanLink,wanLink=perf' + c + '_' + s})
UNWIND range(0, (s + 1) % 3) AS off
MATCH (n:NetworkElement {tag:'class=cluster,cluster=network,network=perf' + c + 'net' + (((s + off) % 1000) + 1)})
CREATE (n)<-[:Connect]-(wl)
;
