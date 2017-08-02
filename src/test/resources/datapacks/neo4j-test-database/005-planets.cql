CREATE (pGlobalApp:Planet        {name:'iwan/global/application/cisco', scope:'class=scope,scope=global', createdAt:timestamp()})
CREATE (pGlobalAppGroup:Planet   {name:'iwan/global/application/group/cisco', scope:'class=scope,scope=global', createdAt:timestamp()})
CREATE (pGlobalAppCat:Planet     {name:'iwan/global/application/category/cisco', scope:'class=scope,scope=global', createdAt:timestamp()})
CREATE (pSPApp:Planet            {name:'iwan/sp/application/cisco', scope:'class=scope,scope=sp', createdAt:timestamp()})
CREATE (pSPAppGroup:Planet       {name:'iwan/sp/application/group/cisco', scope:'class=scope,scope=sp', createdAt:timestamp()})
CREATE (pSPAppCat:Planet         {name:'iwan/sp/application/category/cisco', scope:'class=scope,scope=sp', createdAt:timestamp()})

CREATE (pClient:Planet           {name:'iwan/1/client/cisco', scope:'class=cluster,client=1,cluster=client', createdAt:timestamp()})
CREATE (pNetwork:Planet          {name:'iwan/1/network/cisco', scope:'class=cluster,client=1,cluster=client', createdAt:timestamp()})
CREATE (pArea:Planet             {name:'iwan/1/area/cisco', scope:'class=cluster,client=1,cluster=client', createdAt:timestamp()})
CREATE (pSite:Planet             {name:'iwan/1/site/cisco', scope:'class=cluster,client=1,cluster=client', createdAt:timestamp()})
CREATE (pCpe:Planet              {name:'iwan/1/cpe/cisco', scope:'class=cluster,client=1,cluster=client', createdAt:timestamp()})
CREATE (pNetflowViewpoint:Planet {name:'iwan/1/viewpoint/cisco', scope:'class=cluster,client=1,cluster=client', createdAt:timestamp()})
CREATE (pSnmpWanLink:Planet      {name:'iwan/1/wanLink/cisco', scope:'class=cluster,client=1,cluster=client', createdAt:timestamp()})

CREATE (pApp:Planet              {name:'iwan/1/application/cisco', scope:'class=cluster,client=1,cluster=client', createdAt:timestamp()})
CREATE (pAppUser:Planet          {name:'iwan/1/application/user/cisco', scope:'class=cluster,client=1,cluster=client', createdAt:timestamp()})
CREATE (pCos:Planet              {name:'iwan/global/cos/cisco', scope:'class=scope,scope=global', createdAt:timestamp()})
CREATE (pDscp:Planet             {name:'iwan/global/dscp/cisco', scope:'class=scope,scope=global', createdAt:timestamp()})

CREATE (pBtClient:Planet         {name:'iwan/boots/client/cisco', scope:'class=cluster,client=boots,cluster=client', createdAt:timestamp()})
CREATE (pBtNetwork:Planet        {name:'iwan/boots/network/cisco', scope:'class=cluster,client=boots,cluster=client', createdAt:timestamp()})
CREATE (pBtArea:Planet           {name:'iwan/boots/area/cisco', scope:'class=cluster,client=boots,cluster=client', createdAt:timestamp()})
CREATE (pBtSite:Planet           {name:'iwan/boots/site/cisco', scope:'class=cluster,client=boots,cluster=client', createdAt:timestamp()})
CREATE (pBtCpe:Planet            {name:'iwan/boots/cpe/cisco', scope:'class=cluster,client=boots,cluster=client', createdAt:timestamp()})
CREATE (pBtViewpoint:Planet      {name:'iwan/boots/viewpoint/cisco', scope:'class=cluster,client=boots,cluster=client', createdAt:timestamp()})
CREATE (pBtWanLink:Planet        {name:'iwan/boots/wanLink/cisco', scope:'class=cluster,client=boots,cluster=client', createdAt:timestamp()})
CREATE (pBtCos:Planet            {name:'iwan/boots/cos/cisco', scope:'class=cluster,client=boots,cluster=client', createdAt:timestamp()})

CREATE (pBtApp:Planet            {name:'iwan/boots/application/cisco', scope:'class=cluster,client=boots,cluster=client', createdAt:timestamp()})
CREATE (pBtAppUser:Planet        {name:'iwan/boots/application/user/cisco', scope:'class=cluster,client=boots,cluster=client', createdAt:timestamp()})

;
