//
// Created by li on 2023/12/21.
//

#ifndef ANET_H
#define ANET_H

#define ANET_OK 0
#define ANET_ERR -1
#define ANET_ERR_LEN 256

int anetTcpConnect(char *err, char *addr, int port);
int anetTcpServer(char *err, int port, char *bindaddr);
int anetAccept(char *err, int serversock, char *ip, int *port);
int anetNonBlock(char *err, int fd);
int anetTcpNoDelay(char *err, int fd);

#endif //ANET_H
