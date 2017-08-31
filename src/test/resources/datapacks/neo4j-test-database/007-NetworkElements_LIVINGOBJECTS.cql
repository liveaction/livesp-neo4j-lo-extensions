// CLIENT LIVING OBJECTS
MATCH (sch:Schema {id:'iwan'})

MATCH (pClient:Planet           {name:'iwan/1/client/cisco'})
MATCH (pNetwork:Planet          {name:'iwan/1/network/cisco'})
MATCH (pArea:Planet             {name:'iwan/1/area/cisco'})
MATCH (pSite:Planet             {name:'iwan/1/site/cisco'})
MATCH (pCpe:Planet              {name:'iwan/1/cpe/cisco'})
MATCH (pNetflowViewpoint:Planet {name:'iwan/1/viewpoint/cisco'})
MATCH (pSnmpWanLink:Planet      {name:'iwan/1/wanLink/cisco'})

CREATE (neClientLO:Element:NetworkElement:Scope {tag:'class=cluster,client=1,cluster=client', name:'Living Objects', _type:'cluster:client', id:'1', createdBy:'system', createdAt:1446047124000})
CREATE (neNetwork01:Element:NetworkElement {tag:'class=cluster,cluster=network,network=1', name:'Network 01', _type:'cluster:network', createdBy:'system', createdAt:1446047124000})
CREATE (neNetwork02:Element:NetworkElement {tag:'class=cluster,cluster=network,network=2', name:'Network 02', _type:'cluster:network', createdBy:'system', createdAt:1446047124000})
CREATE (neNetwork03:Element:NetworkElement {tag:'class=cluster,cluster=network,network=3', name:'Network 03', _type:'cluster:network', createdBy:'system', createdAt:1446047124000})

CREATE (neAreaNa:Element:NetworkElement {tag:'area=NorthAmerica,class=cluster,cluster=area', name:'North America', _type:'cluster:area', createdBy:'system', createdAt:1446047124000})
CREATE (neAreaEu:Element:NetworkElement {tag:'area=Europe,class=cluster,cluster=area', name:'Europe', _type:'cluster:area', createdBy:'system', createdAt:1446047124000})

CREATE (neSite1:Element:NetworkElement {tag:'class=cluster,cluster=site,site=Site_1', name:'Site_1', _type:'cluster:site', createdBy:'system', createdAt:1446047124000})
CREATE (neSite2:Element:NetworkElement {tag:'class=cluster,cluster=site,site=Site_2', name:'Site_2', _type:'cluster:site', createdBy:'system', createdAt:1446047124000})
CREATE (neSite3:Element:NetworkElement {tag:'class=cluster,cluster=site,site=Site_3', name:'Site_3', _type:'cluster:site', createdBy:'system', createdAt:1446047124000})
CREATE (neSite4:Element:NetworkElement {tag:'class=cluster,cluster=site,site=Site_4', name:'Site_4', _type:'cluster:site', createdBy:'system', createdAt:1446047124000})

CREATE (neCpe1:Element:NetworkElement {tag:'class=neType,cpe=CC_RJ45,neType=cpe', name:'CC_RJ45', _type:'neType:cpe', id:'CC_RJ45', ip:'172.17.10.31', recipientId:1, createdBy:'system', createdAt:1446047124000})
CREATE (neCpe2:Element:NetworkElement {tag:'class=neType,cpe=HW_3615,neType=cpe', name:'HW_3615', _type:'neType:cpe', id:'HW_3615', ip:'172.17.10.32', recipientId:1, createdBy:'system', createdAt:1446047124000})

CREATE (neCpe3:Element:NetworkElement {tag:'class=neType,cpe=AA_RJ45,neType=cpe', name:'AA_RJ45', _type:'neType:cpe', id:'AA_RJ45', ip:'172.17.10.41', recipientId:1, createdAt:1446047124000})
CREATE (neCpe4:Element:NetworkElement {tag:'class=neType,cpe=BB_3615,neType=cpe', name:'BB_3615', _type:'neType:cpe', ip:'172.17.10.42', recipientId:1, createdAt:1446047124000})
CREATE (neCpe5:Element:NetworkElement {tag:'class=neType,cpe=CPE_WITH_NO_WAN_LINKS,neType=cpe', name:'CPE_WITH_NO_WAN_LINKS', _type:'neType:cpe', ip:'172.17.10.43', recipientId:1, createdAt:1446047124000})
CREATE (neCpe6:Element:NetworkElement {tag:'class=neType,cpe=cpe6,neType=cpe', name:'cpe6', _type:'neType:cpe', ip:'172.17.11.6', recipientId:1, createdAt:1446047124000})

CREATE (neWanLink1:Element:NetworkElement {tag:'class=neType,neType=wanLink,wanLink=CC__RJ45_I/42', name:'I/42', _type:'neType:wanLink', pollingIfIndex:1, createdAt:1446047124000, updatedAt:1446047124000, peList:['172.17.17.1', '172.17.17.18'], bandwidthIn:10000, bandwidthOut:10000})
CREATE (neWanLink2:Element:NetworkElement {tag:'class=neType,neType=wanLink,wanLink=CC__RJ45_I/43', name:'I/43', _type:'neType:wanLink', pollingIfIndex:2, createdAt:1446047124000, updatedAt:1446047249000, bandwidthIn:10000, bandwidthOut:10000})
CREATE (neWanLink3:Element:NetworkElement {tag:'class=neType,neType=wanLink,wanLink=HW__3615_R/42', name:'R/42', _type:'neType:wanLink', pollingIfIndex:1, createdAt:1446047124000, bandwidthIn:10000, bandwidthOut:10000})
CREATE (neWanLink4:Element:NetworkElement {tag:'class=neType,neType=wanLink,wanLink=HW__3615_R/43', name:'R/43', _type:'neType:wanLink', pollingIfIndex:2, createdAt:1446047124000, updatedAt:1446047249000, bandwidthIn:10000, bandwidthOut:10000})

CREATE (neWanLink5:Element:NetworkElement {tag:'class=neType,neType=wanLink,wanLink=cpe6_wanLink5', name:'cpe6_wanLink5', _type:'neType:wanLink', pollingIfIndex:2, createdAt:1446047124000, updatedAt:1446047249000, bandwidthIn:10000, bandwidthOut:10000})
CREATE (neWanLink6:Element:NetworkElement {tag:'class=neType,neType=wanLink,wanLink=cpe6_wanLink6', name:'cpe6_wanLink6', _type:'neType:wanLink', pollingIfIndex:3, createdAt:1446047124000, updatedAt:1446047249000, bandwidthIn:10000, bandwidthOut:10000})
CREATE (neWanLink7:Element:NetworkElement {tag:'class=neType,neType=wanLink,wanLink=cpe6_wanLink7', name:'cpe6_wanLink7', _type:'neType:wanLink', pollingIfIndex:3, createdAt:1446047124000, updatedAt:1446047249000, bandwidthIn:10000, bandwidthOut:10000})

CREATE (neViewpoint5:Element:NetworkElement {tag:'class=neType,neType=viewpoint,viewpoint=AA__RJ45_I/44', name:'I/44', _type:'neType:viewpoint', ifIndexes:[1], createdAt:1446047124000})
CREATE (neViewpoint6:Element:NetworkElement {tag:'class=neType,neType=viewpoint,viewpoint=AA__RJ45_I/45', name:'I/45', _type:'neType:viewpoint', ifIndexes:[2], createdAt:1446047124000})
CREATE (neViewpoint7:Element:NetworkElement {tag:'class=neType,neType=viewpoint,viewpoint=BB__3615_R/44', name:'R/44', _type:'neType:viewpoint', ifIndexes:[1], createdAt:1446047124000})
CREATE (neViewpoint8:Element:NetworkElement {tag:'class=neType,neType=viewpoint,viewpoint=BB__3615_R/45', name:'R/45', _type:'neType:viewpoint', ifIndexes:[2], createdAt:1446047124000})

CREATE (neViewpoint9:Element:NetworkElement {tag:'class=neType,neType=viewpoint,viewpoint=viewpoint9', name:'viewpoint9', _type:'neType:viewpoint', ifIndexes:[5,6,7], createdAt:1446047124000})
CREATE (neViewpoint10:Element:NetworkElement {tag:'class=neType,neType=viewpoint,viewpoint=viewpoint10', name:'viewpoint10', _type:'neType:viewpoint', ifIndexes:[8,9], createdAt:1446047124000})
CREATE (neViewpoint11:Element:NetworkElement {tag:'class=neType,neType=viewpoint,viewpoint=viewpoint11', name:'viewpoint11', _type:'neType:viewpoint', ifIndexes:[10,11,12,13,14], createdAt:1446047124000})
CREATE (neViewpoint12:Element:NetworkElement {tag:'class=neType,neType=viewpoint,viewpoint=viewpoint12', name:'viewpoint12', _type:'neType:viewpoint', ifIndexes:[15,16], createdAt:1446047124000})

// Site without area
CREATE (neSite5:Element:NetworkElement {tag:'class=cluster,cluster=site,site=Site_5', name:'Site_5', _type:'cluster:site', createdBy:'system', createdAt:1446047124000})
CREATE (neCpe7:Element:NetworkElement {tag:'class=neType,cpe=NA_4233,neType=cpe', name:'NA_4233', _type:'neType:cpe', ip:'172.17.11.66', recipientId:1, createdAt:1446047124000})
CREATE (neViewpoint13:Element:NetworkElement {tag:'class=neType,neType=viewpoint,viewpoint=viewpoint13', name:'viewpoint_sans_wanlink', _type:'neType:viewpoint', ifIndexes:[15,16], createdAt:1446047124000})

CREATE (neClientLO)<-[:AppliedTo]-(sch)

CREATE (pClient)<-[:Attribute]-(neClientLO)

CREATE (neCpe1)<-[:Connect]-(neWanLink1)
CREATE (neCpe1)<-[:Connect]-(neWanLink2)
CREATE (neCpe2)<-[:Connect]-(neWanLink3)
CREATE (neCpe2)<-[:Connect]-(neWanLink4)

CREATE (neCpe3)<-[:Connect]-(neViewpoint5)
CREATE (neCpe3)<-[:Connect]-(neViewpoint6)
CREATE (neCpe4)<-[:Connect]-(neViewpoint7)
CREATE (neCpe4)<-[:Connect]-(neViewpoint8)

CREATE (neCpe6)<-[:Connect]-(neWanLink5)
CREATE (neCpe6)<-[:Connect]-(neWanLink6)
CREATE (neCpe6)<-[:Connect]-(neWanLink7)

CREATE (neCpe6)<-[:Connect]-(neViewpoint9)
CREATE (neCpe6)<-[:Connect]-(neViewpoint10)
CREATE (neCpe6)<-[:Connect]-(neViewpoint11)
CREATE (neCpe6)<-[:Connect]-(neViewpoint12)
CREATE (neCpe7)<-[:Connect]-(neViewpoint13)

CREATE (neWanLink5)<-[:Connect]-(neViewpoint9)
CREATE (neWanLink5)<-[:Connect]-(neViewpoint10)
CREATE (neWanLink6)<-[:Connect]-(neViewpoint11)

CREATE (pCpe)<-[:Attribute]-(neCpe1)
CREATE (pCpe)<-[:Attribute]-(neCpe2)
CREATE (pCpe)<-[:Attribute]-(neCpe3)
CREATE (pCpe)<-[:Attribute]-(neCpe4)
CREATE (pCpe)<-[:Attribute]-(neCpe5)
CREATE (pCpe)<-[:Attribute]-(neCpe6)
CREATE (pCpe)<-[:Attribute]-(neCpe7)

CREATE (neNetwork01)<-[:Connect]-(neWanLink1)
CREATE (neNetwork01)<-[:Connect]-(neWanLink3)
CREATE (neNetwork02)<-[:Connect]-(neWanLink2)
CREATE (neNetwork02)<-[:Connect]-(neWanLink4)

CREATE (neNetwork03)<-[:Connect]-(neWanLink5)
CREATE (neNetwork03)<-[:Connect]-(neWanLink6)

CREATE (pNetwork)<-[:Attribute]-(neNetwork01)
CREATE (pNetwork)<-[:Attribute]-(neNetwork02)

CREATE (neSite1)<-[:Connect]-(neCpe1)
CREATE (neSite1)<-[:Connect]-(neCpe2)
CREATE (pSite)<-[:Attribute]-(neSite1)
CREATE (pSite)<-[:Attribute]-(neSite2)
CREATE (pSite)<-[:Attribute]-(neSite3)
CREATE (pSite)<-[:Attribute]-(neSite4)
CREATE (pSite)<-[:Attribute]-(neSite5)

CREATE (neSite2)<-[:Connect]-(neCpe3)
CREATE (neSite2)<-[:Connect]-(neCpe4)
CREATE (neSite2)<-[:Connect]-(neCpe5)
CREATE (neSite4)<-[:Connect]-(neCpe6)
CREATE (neSite5)<-[:Connect]-(neCpe7)

CREATE (neClientLO)<-[:Connect]-(neSite1)
CREATE (neClientLO)<-[:Connect]-(neSite2)
CREATE (neClientLO)<-[:Connect]-(neSite3)
CREATE (neClientLO)<-[:Connect]-(neSite4)
CREATE (neClientLO)<-[:Connect]-(neSite5)

CREATE (neClientLO)<-[:Connect]-(neNetwork01)
CREATE (neClientLO)<-[:Connect]-(neNetwork02)
CREATE (neClientLO)<-[:Connect]-(neNetwork03)

CREATE (neAreaEu)<-[:Connect]-(neSite1)
CREATE (neAreaEu)<-[:Connect]-(neSite4)
CREATE (neAreaNa)<-[:Connect]-(neSite2)
CREATE (neAreaNa)<-[:Connect]-(neSite3)

CREATE (neClientLO)<-[:Connect]-(neAreaEu)
CREATE (neClientLO)<-[:Connect]-(neAreaNa)

CREATE (pNetflowViewpoint)<-[:Attribute]-(neViewpoint5)
CREATE (pNetflowViewpoint)<-[:Attribute]-(neViewpoint6)
CREATE (pNetflowViewpoint)<-[:Attribute]-(neViewpoint7)
CREATE (pNetflowViewpoint)<-[:Attribute]-(neViewpoint8)
CREATE (pNetflowViewpoint)<-[:Attribute]-(neViewpoint9)
CREATE (pNetflowViewpoint)<-[:Attribute]-(neViewpoint10)
CREATE (pNetflowViewpoint)<-[:Attribute]-(neViewpoint11)
CREATE (pNetflowViewpoint)<-[:Attribute]-(neViewpoint12)
CREATE (pNetflowViewpoint)<-[:Attribute]-(neViewpoint13)

CREATE (pArea)<-[:Attribute]-(neAreaNa)
CREATE (pArea)<-[:Attribute]-(neAreaEu)

CREATE (pSnmpWanLink)<-[:Attribute]-(neWanLink1)
CREATE (pSnmpWanLink)<-[:Attribute]-(neWanLink2)
CREATE (pSnmpWanLink)<-[:Attribute]-(neWanLink3)
CREATE (pSnmpWanLink)<-[:Attribute]-(neWanLink4)
CREATE (pSnmpWanLink)<-[:Attribute]-(neWanLink5)
CREATE (pSnmpWanLink)<-[:Attribute]-(neWanLink6)
CREATE (pSnmpWanLink)<-[:Attribute]-(neWanLink7)

;
