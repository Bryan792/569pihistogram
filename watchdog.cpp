#include <sys/time.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <sys/socket.h>
#include <resolv.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/ip_icmp.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#define PACKETSIZE  64
#define bzero(b,len) (memset((b), '\0', (len)), (void) 0)
struct packet
{
  struct icmphdr hdr;
  //char msg[PACKETSIZE-sizeof(struct icmphdr)];
  char msg[56];
};

char *f1, *f2;

pid_t pid;
timeval time[6];

unsigned short checksum(void *b, int len)
{
  unsigned short *buf = (unsigned short *)b;
  unsigned int sum = 0;
  unsigned short result;

  for ( sum = 0; len > 1; len -= 2 )
  {
    sum += *buf++;
  }

  if ( len == 1 )
  {
    sum += *(unsigned char*)buf;
  }

  sum = (sum >> 16) + (sum & 0xFFFF);
  sum += (sum >> 16);
  result = ~sum;
  return result;
}




int killstuff()
{
  kill(pid, SIGINT);
}

int forkandexec()
{
  pid = fork();

  if(pid == 0)
  {
    //sleep(5);
   // execl("/bin/touch", "touch", "test", (char*)0);
    execl("/usr/local/bin/mpirun","mpirun","-host","rpi1,rpi2,rpi3,rpi4,rpi5","-n","5","-prefix","/usr/local","histogram",f1,f2,(char *)0);
  }
  else
  {
    return pid;
  }
}

int sd;
struct sockaddr_in addr[6];
struct sockaddr_in r_addr[6];
//struct packet pckt[5];
struct protoent *proto = NULL;


void sendping(int i)
{
  struct packet pckt;
  int cnt = 1;
int j;
  bzero(&pckt, sizeof(pckt));
  pckt.hdr.type = ICMP_ECHO;
  pckt.hdr.un.echo.id = pid;

  for ( j = 0; j < sizeof(pckt.msg) - 1; j++ )
  {
    pckt.msg[i] = i + '0';
  }

  pckt.msg[i] = 0;
  pckt.hdr.un.echo.sequence = cnt++;
  pckt.hdr.checksum = checksum(&pckt, sizeof(pckt));

  if ( sendto(sd, &pckt, sizeof(pckt), 0, (struct sockaddr*)&(addr[i]), sizeof(struct sockaddr)) <= 0 )
  {
    perror("sendto");
  }
  printf("send %i\n", addr[i].sin_addr.s_addr);
  return;
}

void recvping(int i)
{
  struct packet pckt2;
  int len = sizeof(r_addr[i]);
  int j;

  if ( recvfrom(sd, &pckt2, sizeof(pckt2), 0, (struct sockaddr*) & (r_addr[i]), (socklen_t*)&len) > 0 )
  {
  
   

//  gettimeofday(&time[i], NULL);
    printf("recv %i\n", r_addr[i].sin_addr.s_addr);


   for (j = 1; j<=5; j++){
        if(r_addr[i].sin_addr.s_addr == addr[j].sin_addr.s_addr){
            gettimeofday(&time[j], NULL); 
            printf("setting time %i\n", j);
        }
   }


  }else{
    printf("no recv\n");
  }

  return;
}

int main(int argc, char *argv[])
{
  f1 = argv[1];
  f2 = argv[2];
  const int val = 255;
  int i, cnt = 1;
//   struct packet pckt[5];
  int ret, status;
  struct hostent *hname;
  gettimeofday(&time[1], NULL);
  gettimeofday(&time[2], NULL);
  gettimeofday(&time[3], NULL);
  gettimeofday(&time[4], NULL);
  gettimeofday(&time[5], NULL);
  forkandexec();
  proto = getprotobyname("ICMP");
  sd = socket(PF_INET, SOCK_RAW, proto->p_proto);

  if ( sd < 0 )
  {
    perror("socket");
    return(1);
  }

  if ( setsockopt(sd, SOL_IP, IP_TTL, &val, sizeof(val)) != 0)
  {
    perror("Set TTL option");
  }

  if ( fcntl(sd, F_SETFL, O_NONBLOCK) != 0 )
  {
    perror("Request nonblocking I/O");
  }

  char name[] = "rpi0";

  for(i = 1; i <= 5; i++)
  {
    name[3] = i + 48;
    hname = gethostbyname(name);
    bzero(&(addr[i]), sizeof(addr[i]));
    addr[i].sin_family = hname->h_addrtype;
    addr[i].sin_port = 0;
    addr[i].sin_addr.s_addr = *(long*)hname->h_addr;


    printf("%i\n", addr[i].sin_addr.s_addr);
  }

  /*
     while(1)
     {
       sendping(1);
       sleep(1);
       recvping(1);
       sendping(2);
       sleep(1);
       recvping(2);
       sendping(3);
       sleep(1);
       recvping(3);
       sendping(4);
       sleep(1);
       recvping(4);
       sendping(5);
       sleep(1);
       recvping(5);
       sendping(1);
       sleep(1);
       recvping(1);
     }
  */
int bad = -1;
  for(;;)
  {
    ret = waitpid(pid, &status, WNOHANG);

    if ( ret == pid)
    {
      if(WIFEXITED(status))
      {
        exit(0);
      }
      else
      {
        //execl("mpirun", "mpirun", "-host", "rpi1,rpi2,rpi3,rpi4,rpi5", "-n", "4", "-prefix", "/usr/local", "histogramb", f1, f2, (char *)0);
        forkandexec();
      }
    }

    for(i = 1; i <= 5; i++)
    {
      sendping(i);
      sleep(1);
      recvping(i);
      recvping(i);
      recvping(i);
      recvping(i);
      recvping(i);


      printf("ping %i \n", i);
      //i think here we do the check?
      gettimeofday(&time[0], NULL);

      if(time[0].tv_sec - time[i].tv_sec > 10)
      {
        bad=i;
        printf("error node dc\n");
      }
    }

    if(bad != -1)
    {
      killstuff();
      char hosts[20] = "rpi0,rpi0,rpi0,rpi0";
int pos = 1;
      for(i = 1; i <= 5; i++)
      {
//        char name[] = "rpi0,";   // 3 8
 
        if (i != bad)
        {
          hosts[5*(pos++)-2] = i + 48;
        }
      }
 
      execl("/usr/local/bin/mpirun", "mpirun", "-host", hosts, "-n", "4", "-prefix", "/usr/local", "histogram", f1, f2, (char *)0);
    }


  }

  return(0);
}
