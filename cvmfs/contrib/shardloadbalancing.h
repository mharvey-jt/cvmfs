#ifndef __SHARD_LOAD_BALANCING
#define __SHARD_LOAD_BALANCING

#include <iostream>
#include <algorithm>
#include <vector>
#include <string>
#include <map>
#include <cstdlib>

#include "util/logging.h"
#include "network/sharding_policy.h"
#include "network/health_check.h"

#include "contrib/sharding.h"

extern "C" {
static void __log_proxy_state( const char*proxy_url,  int state,int N ) {
LogCvmfs( kLogDownload, kLogDebug, "ShardLoadBalancing::ProxyStateChange Proxy '%s' state=%s . There are %d proxies online", proxy_url, state==1? "ONLINE": "OFFLINE", N ); 
}
}

class ShardLoadBalancing : public download::ShardingPolicy, public download::HealthCheck {

private: 

struct shard_t *shard_;
public:

ShardLoadBalancing() {
  shard_ = shard_init( SHARD_CONFIG_NONE );
  shard_set_logging_function( shard_, &__log_proxy_state );
}
~ShardLoadBalancing() {
 shard_free( shard_ );
  LogCvmfs( kLogDownload, kLogDebug, "ShardLoadBalancing::~ShardLoadBalancing: Finished" );
}

void StartHealthcheck() {
  LogCvmfs( kLogDownload, kLogDebug, "ShardLoadBalancing::StartHealthcheck: starting healthcheck thread" );
  shard_healthcheck_start(shard_);
}

void StopHealthcheck() {
  LogCvmfs( kLogDownload, kLogDebug, "ShardLoadBalancing::StopHealthcheck: stopping healthcheck thread" );
  shard_healthcheck_stop(shard_);
}


void AddProxy( std::string proxy ) {
  shard_add_proxy( shard_, proxy.c_str() );

  LogCvmfs( kLogDownload, kLogDebug, "ShardLoadBalancing::AddProxy: Adding '%s'. There are %d proxies defined", proxy.c_str(), shard_->N );
}


int GetNumberOfProxiesOnline() {
  return shard_get_number_of_proxies_online(shard_);
}

std::string GetNextProxy( const std::string *url, std::string current_proxy, size_t off ) {
 char **pref =shard_path( shard_, url->c_str(), off );

 std::string ret;
 int idx=0;

 if(NULL==pref) { return ret; }

 if( current_proxy != "" ) {
   LogCvmfs(kLogDownload, kLogDebug, "ShardLoadBalancing::GetNextProxy: setting proxy '%s' as down. It will be polled and returned to service automatically. There are %d proxies online ", current_proxy.c_str(), GetNumberOfProxiesOnline() );
   shard_set_proxy_offline( shard_, current_proxy.c_str() );  
 }

 ret = std::string(pref[0]);

 for(unsigned int i=0; i < shard_->N; i++ ) {
   if(!strcmp( current_proxy.c_str(), pref[i] ) ) {
     idx = (i+1) % shard_->N;
     ret =  std::string( pref[idx] );
     break;
   }
 }

 free(pref);

 LogCvmfs( kLogDownload, kLogDebug, "ShardLoadBalancing::GetNextProxy: Path '%s' with offset %lu is sharded with preferred proxy '%s' (index %d) ", url->c_str(), off,  ret.c_str(), idx );
 return ret;
}

void LogProxyList() {
 for(unsigned int i=0; i<shard_->N; i++ ) {
  LogCvmfs(kLogDownload, kLogDebug, "ShardLoadBalancing::LogProxyList %d '%s'", i,  shard_->proxy_url[i] );
 }
}


std::string GetProxyList(void) {
  std::string ret;
  for(unsigned int i=0; i<shard_->N; i++ ) {
    ret += std::string( shard_->proxy_url[i]) +"\n";
  }
  return ret;

}

std::string GetPreferredProxyList( std::string url ) {
  std::string ret;
  char **pref =shard_path( shard_, url.c_str(), 0 );

  for(unsigned int i=0; i<shard_->N; i++ ) {
    ret += std::string(pref[i]) +"\n";
  }
  free(pref);
  return ret;

}

};

#endif

