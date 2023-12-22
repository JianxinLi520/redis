//
// Created by li on 2023/12/21.
//

#ifndef ANET_H
#define ANET_H

#define ANET_OK 0
#define ANET_ERR -1
#define ANET_ERR_LEN 256

int anetTcpServer(char *err, int port, char *bindaddr);

#endif //ANET_H
