MATCH (sch:Schema {id:'iwan'})

MATCH (pBtClient:Planet         {name:'iwan/boots/client/cisco'})
MATCH (pBtNetwork:Planet        {name:'iwan/boots/network/cisco'})
MATCH (pBtArea:Planet           {name:'iwan/boots/area/cisco'})
MATCH (pBtSite:Planet           {name:'iwan/boots/site/cisco'})
MATCH (pBtCpe:Planet            {name:'iwan/boots/cpe/cisco'})
MATCH (pBtViewpoint:Planet      {name:'iwan/boots/viewpoint/cisco'})
MATCH (pBtWanLink:Planet        {name:'iwan/boots/wanLink/cisco'})

CREATE (neClientBoots:Element:NetworkElement:Scope {tag:'class=cluster,client=boots,cluster=client', name:'Boots', id:'boots', _type:'cluster:client', createdAt:1453192872000})
CREATE (neNetworkBt01:Element:NetworkElement {tag:'class=cluster,client=boots,cluster=network,network=1', name:'Network BT 01', _type:'cluster:network', createdAt:1453192872000})
CREATE (neNetworkBt02:Element:NetworkElement {tag:'class=cluster,client=boots,cluster=network,network=2', name:'Network BT 02', _type:'cluster:network', createdAt:1453192872000})

CREATE (neAreaUk:Element:NetworkElement {tag:'area=UK,class=cluster,client=boots,cluster=area', name:'United Kingdom', _type:'cluster:area', createdAt:1453192872000})
CREATE (neAreaBtEu:Element:NetworkElement {tag:'area=Europe,class=cluster,client=boots,cluster=area', name:'Europe', _type:'cluster:area', createdAt:1453192872000})

CREATE (neSiteBt1:Element:NetworkElement {tag:'class=cluster,client=boots,cluster=site,site=Site_1', name:'SiteBT_1', _type:'cluster:site', createdAt:1453192872000})
CREATE (neSiteBt2:Element:NetworkElement {tag:'class=cluster,client=boots,cluster=site,site=Site_2', name:'SiteBT_2', _type:'cluster:site', createdAt:1453192872000})
CREATE (neSiteBt3:Element:NetworkElement {tag:'class=cluster,client=boots,cluster=site,site=Site_3', name:'SiteBT_3', _type:'cluster:site', createdAt:1453192872000})

CREATE (neCpeBt1:Element:NetworkElement {tag:'class=neType,cpe=CC_RJ46,neType=cpe', name:'CC_RJ46', _type:'neType:cpe', ip:'172.17.11.23', recipientId:1, createdAt:1453192872000})
CREATE (neCpeBt2:Element:NetworkElement {tag:'class=neType,cpe=HW_3616,neType=cpe', name:'HW_3616', _type:'neType:cpe', ip:'172.17.11.24', recipientId:1, createdAt:1453192872000})

CREATE (neViewpointBt1:Element:NetworkElement {tag:'class=neType,neType=viewpoint,viewpoint=CC__RJ46_I/42', name:'I/42', _type:'neType:viewpoint', ip:'172.17.11.23', ifIndexes:[1], createdAt:1453192872000, bandwidthIn:10000, bandwidthOut:10000})
CREATE (neViewpointBt2:Element:NetworkElement {tag:'class=neType,neType=viewpoint,viewpoint=CC__RJ46_I/43', name:'I/43', _type:'neType:viewpoint', ip:'172.17.11.23', ifIndexes:[2], createdAt:1453192872000, updatedAt:1453192879000, bandwidthIn:10000, bandwidthOut:10000})
CREATE (neViewpointBt3:Element:NetworkElement {tag:'class=neType,neType=viewpoint,viewpoint=HW__3616_R/42', name:'R/42', _type:'neType:viewpoint', ip:'172.17.11.24', ifIndexes:[1], createdAt:1453192872000, bandwidthIn:10000, bandwidthOut:10000})
CREATE (neViewpointBt4:Element:NetworkElement {tag:'class=neType,neType=viewpoint,viewpoint=HW__3616_R/43', name:'R/43', _type:'neType:viewpoint', ip:'172.17.11.24', ifIndexes:[2], createdAt:1453192872000, updatedAt:1453192879000, bandwidthIn:10000, bandwidthOut:10000})

CREATE (neCpeBt3:Element:NetworkElement {tag:'class=neType,cpe=AA_RJ46,neType=cpe', name:'AA_RJ46', _type:'neType:cpe', ip:'172.17.11.25', recipientId:1, createdAt:1453192872000})
CREATE (neCpeBt4:Element:NetworkElement {tag:'class=neType,cpe=BB_3616,neType=cpe', name:'BB_3616', _type:'neType:cpe', ip:'172.17.11.26', recipientId:1, createdAt:1453192872000})
CREATE (neCpeBt5:Element:NetworkElement {tag:'class=neType,cpe=CPE_WITH_NO_VIEW_POINTS_BT,neType=cpe', name:'CPE_WITH_NO_VIEW_POINTS_BT', _type:'neType:cpe', ip:'172.17.11.27', recipientId:1, createdAt:1453192872000})

CREATE (neViewpointBt6:Element:NetworkElement {tag:'class=neType,neType=viewpoint,viewpoint=AA__RJ46_I/45', name:'I/45', _type:'neType:viewpoint', ip:'172.17.11.25', ifIndexes:[2], createdAt:1453192872000})
CREATE (neViewpointBt5:Element:NetworkElement {tag:'class=neType,neType=viewpoint,viewpoint=AA__RJ46_I/44', name:'I/44', _type:'neType:viewpoint', ip:'172.17.11.25', ifIndexes:[1], createdAt:1453192872000})
CREATE (neViewpointBt8:Element:NetworkElement {tag:'class=neType,neType=viewpoint,viewpoint=BB__3616_R/45', name:'R/45', _type:'neType:viewpoint', ip:'172.17.11.26', ifIndexes:[2], createdAt:1453192872000})
CREATE (neViewpointBt7:Element:NetworkElement {tag:'class=neType,neType=viewpoint,viewpoint=BB__3616_R/44', name:'R/44', _type:'neType:viewpoint', ip:'172.17.11.26', ifIndexes:[1], createdAt:1453192872000})

CREATE (neClientBoots)<-[:AppliedTo]-(sch)

CREATE (pBtClient)<-[:Attribute]-(neClientBoots)

CREATE (neCpeBt1)<-[:Connect]-(neViewpointBt1)
CREATE (neCpeBt1)<-[:Connect]-(neViewpointBt2)
CREATE (neCpeBt2)<-[:Connect]-(neViewpointBt3)
CREATE (neCpeBt2)<-[:Connect]-(neViewpointBt4)

CREATE (neCpeBt3)<-[:Connect]-(neViewpointBt5)
CREATE (neCpeBt3)<-[:Connect]-(neViewpointBt6)
CREATE (neCpeBt4)<-[:Connect]-(neViewpointBt7)
CREATE (neCpeBt4)<-[:Connect]-(neViewpointBt8)

CREATE (pBtCpe)<-[:Attribute]-(neCpeBt1)
CREATE (pBtCpe)<-[:Attribute]-(neCpeBt2)
CREATE (pBtCpe)<-[:Attribute]-(neCpeBt3)
CREATE (pBtCpe)<-[:Attribute]-(neCpeBt4)

CREATE (pBtViewpoint)<-[:Attribute]-(neViewpointBt1)
CREATE (pBtViewpoint)<-[:Attribute]-(neViewpointBt2)
CREATE (pBtViewpoint)<-[:Attribute]-(neViewpointBt3)
CREATE (pBtViewpoint)<-[:Attribute]-(neViewpointBt4)
CREATE (pBtViewpoint)<-[:Attribute]-(neViewpointBt5)
CREATE (pBtViewpoint)<-[:Attribute]-(neViewpointBt6)
CREATE (pBtViewpoint)<-[:Attribute]-(neViewpointBt7)
CREATE (pBtViewpoint)<-[:Attribute]-(neViewpointBt8)

CREATE (neNetworkBt01)<-[:Connect]-(neViewpointBt1)
CREATE (neNetworkBt01)<-[:Connect]-(neViewpointBt3)
CREATE (neNetworkBt01)<-[:Connect]-(neViewpointBt5)
CREATE (neNetworkBt01)<-[:Connect]-(neViewpointBt7)
CREATE (neNetworkBt01)<-[:Connect]-(neViewpointBt8)
CREATE (neNetworkBt02)<-[:Connect]-(neViewpointBt2)
CREATE (neNetworkBt02)<-[:Connect]-(neViewpointBt4)
CREATE (neNetworkBt02)<-[:Connect]-(neViewpointBt6)
CREATE (pBtNetwork)<-[:Attribute]-(neNetworkBt01)
CREATE (pBtNetwork)<-[:Attribute]-(neNetworkBt02)

CREATE (neSiteBt1)<-[:Connect]-(neCpeBt1)
CREATE (neSiteBt1)<-[:Connect]-(neCpeBt2)

CREATE (neSiteBt2)<-[:Connect]-(neCpeBt3)
CREATE (neSiteBt2)<-[:Connect]-(neCpeBt4)
CREATE (neSiteBt2)<-[:Connect]-(neCpeBt5)

CREATE (pBtSite)<-[:Attribute]-(neSiteBt1)
CREATE (pBtSite)<-[:Attribute]-(neSiteBt2)
CREATE (pBtSite)<-[:Attribute]-(neSiteBt3)

CREATE (neClientBoots)<-[:Connect]-(neSiteBt1)
CREATE (neClientBoots)<-[:Connect]-(neSiteBt2)
CREATE (neClientBoots)<-[:Connect]-(neSiteBt3)
CREATE (neClientBoots)<-[:Connect]-(neNetworkBt01)
CREATE (neClientBoots)<-[:Connect]-(neNetworkBt02)

CREATE (neAreaBtEu)<-[:Connect]-(neSiteBt1)
CREATE (neAreaUk)<-[:Connect]-(neSiteBt2)
CREATE (neAreaUk)<-[:Connect]-(neSiteBt3)

CREATE (neClientBoots)<-[:Connect]-(neAreaBtEu)
CREATE (neClientBoots)<-[:Connect]-(neAreaUk)

CREATE (pBtArea)<-[:Attribute]-(neAreaUk)
CREATE (pBtArea)<-[:Attribute]-(neAreaBtEu)
;

