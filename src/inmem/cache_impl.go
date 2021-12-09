package inmem

import (
	"github.com/coocood/freecache"
	pb "github.com/envoyproxy/go-control-plane/envoy/service/ratelimit/v3"
	"github.com/envoyproxy/ratelimit/src/config"
	"github.com/envoyproxy/ratelimit/src/limiter"
	"github.com/envoyproxy/ratelimit/src/settings"
	"github.com/envoyproxy/ratelimit/src/stats"
	"github.com/envoyproxy/ratelimit/src/utils"
	gostats "github.com/lyft/gostats"
	"golang.org/x/net/context"
	"math/rand"
	"sync"
)

type inmemCacheImpl struct {
	baseRateLimiter *limiter.BaseRateLimiter
}

var segmentedHits map[int64]map[string]uint32 = make(map[int64]map[string]uint32) // keySegment to (key to counter)
var currentKeySegments map[int64]int64 = make(map[int64]int64) // divider to keySegment
var segmentLock sync.Mutex = sync.Mutex{}

func (this *inmemCacheImpl) DoLimit(
	ctx context.Context,
	request *pb.RateLimitRequest,
	limits []*config.RateLimit) []*pb.RateLimitResponse_DescriptorStatus {

	result := make([]*pb.RateLimitResponse_DescriptorStatus, len(limits))
	hitsAddend := utils.Max(1, request.HitsAddend)
	cacheKeys, now := this.baseRateLimiter.GenerateCacheKeys(request, limits, hitsAddend)

	for idx, limit := range limits {
		keyName := cacheKeys[idx].Key
		var limitBeforeIncrease, limitAfterIncrease uint32
		if limit != nil {
			limitBeforeIncrease, limitAfterIncrease = updateCounters(limit.Limit.Unit, now, keyName, request.HitsAddend)
		} else {
			limitBeforeIncrease, limitAfterIncrease = 0, 1
		}

		limitInfo := limiter.NewRateLimitInfo(limit, limitBeforeIncrease, limitAfterIncrease, 0, 0)
		result[idx] = this.baseRateLimiter.GetResponseDescriptorStatus(keyName,
			limitInfo, false, request.HitsAddend)
	}

	return result
}

func updateCounters(unit pb.RateLimitResponse_RateLimit_Unit, now int64, keyName string, hitsAddend uint32) (uint32, uint32) {
	divider := utils.UnitToDivider(unit)
	keySegment := (now / divider) * divider

	segmentLock.Lock()
	var currentKeySegment = currentKeySegments[divider]
	if currentKeySegment == 0 {
		currentKeySegments[divider] = keySegment
	} else if currentKeySegment != keySegment {
		delete(segmentedHits, currentKeySegment)
		currentKeySegments[divider] = keySegment
	}

	dividerCounters := segmentedHits[keySegment]
	if dividerCounters == nil {
		dividerCounters = make(map[string]uint32)
		segmentedHits[keySegment] = dividerCounters
	}
	limitBeforeIncrease := dividerCounters[keyName]
	limitAfterIncrease := limitBeforeIncrease + hitsAddend
	dividerCounters[keyName] = limitAfterIncrease
	segmentLock.Unlock()

	return limitBeforeIncrease, limitAfterIncrease
}

func (this *inmemCacheImpl) Flush() {}

func NewRateLimiterCacheImplFromSettings(s settings.Settings, timeSource utils.TimeSource, jitterRand *rand.Rand,
	localCache *freecache.Cache, scope gostats.Scope, statsManager stats.Manager) limiter.RateLimitCache {
	return &inmemCacheImpl{
		baseRateLimiter: limiter.NewBaseRateLimit(timeSource, jitterRand, s.ExpirationJitterMaxSeconds, localCache, s.NearLimitRatio, s.CacheKeyPrefix, statsManager),
	}
}
