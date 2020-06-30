package api

import (
	"context"
	"github.com/NYTimes/gziphandler"
	"github.com/timescale/timescale-prometheus/pkg/query"
	"math"
	"net/http"
)

func Labels(queriable *query.Queryable) http.Handler {
	hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")

		querier, err := queriable.Querier(context.Background(), math.MinInt64, math.MaxInt64)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err, "internal")
			return
		}
		names, warnings, err := querier.LabelNames()
		if err != nil {
			respondError(w, http.StatusInternalServerError, err, "internal")
			return
		}

		respond(w, names, warnings)
	})

	return gziphandler.GzipHandler(hf)
}