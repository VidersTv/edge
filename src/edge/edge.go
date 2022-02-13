package edge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/fasthttp/router"
	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	"github.com/viderstv/common/structures"
	"github.com/viderstv/common/svc/mongo"
	"github.com/viderstv/common/utils"
	"github.com/viderstv/edge/loaders"
	"github.com/viderstv/edge/src/global"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func New(gCtx global.Context) <-chan struct{} {
	done := make(chan struct{})

	router := router.New()

	loader := loaders.NewUserLoader(loaders.UserLoaderConfig{
		Fetch: func(keys []primitive.ObjectID) ([]*structures.User, []error) {
			ctx, cancel := context.WithTimeout(gCtx, time.Second*10)
			defer cancel()
			cur, err := gCtx.Inst().Mongo.Collection(mongo.CollectionNameUsers).Find(ctx, bson.M{
				"_id": bson.M{
					"$in": keys,
				},
			})
			dbUsers := []structures.User{}
			if err == nil {
				err = cur.All(ctx, &dbUsers)
			}
			users := make([]*structures.User, len(keys))
			errs := make([]error, len(keys))
			if err != nil {
				logrus.Error("failed to fetch users: ", err)
				for i := range errs {
					errs[i] = err
				}
				return users, errs
			}

			mp := map[primitive.ObjectID]structures.User{}
			for _, v := range dbUsers {
				mp[v.ID] = v
			}

			for i, v := range keys {
				if user, ok := mp[v]; ok {
					users[i] = &user
				} else {
					errs[i] = mongo.ErrNoDocuments
				}
			}

			return users, errs
		},
		Wait: time.Millisecond * 50,
	})

	srv := fasthttp.Server{
		Handler: func(ctx *fasthttp.RequestCtx) {
			start := time.Now()
			defer func() {
				err := recover()
				if err != nil {
					ctx.SetStatusCode(500)
				}

				ctx.Response.Header.Set("Access-Control-Allow-Origin", "*")
				ctx.Response.Header.Set("Access-Control-Allow-Methods", "GET")
				ctx.Response.Header.Set("Access-Control-Max-Age", "86400")

				ctx.Response.Header.Set("X-Edge-Pod-Name", gCtx.Config().Pod.Name)
				ctx.Response.Header.Set("Server", "Viders")
				ctx.Response.Header.Del("connection")

				l := logrus.WithFields(logrus.Fields{
					"status":     ctx.Response.StatusCode(),
					"path":       utils.B2S(ctx.Request.RequestURI()),
					"duration":   time.Since(start) / time.Millisecond,
					"ip":         utils.B2S(ctx.Request.Header.Peek("cf-connecting-ip")),
					"method":     utils.B2S(ctx.Method()),
					"entrypoint": "api",
				})
				if err != nil {
					l.Error("panic in handler: ", err)
					ctx.SetBodyString("Internal Server Error")
					return
				} else {
					l.Info("")
				}
			}()

			router.Handler(ctx)
		},
		ReadTimeout:     time.Second * 10,
		WriteTimeout:    time.Second * 10,
		IdleTimeout:     time.Second * 10,
		GetOnly:         true,
		Name:            "Viders",
		CloseOnShutdown: true,
	}

	router.GET("/{stream}/master.m3u8", func(ctx *fasthttp.RequestCtx) {
		streamID, err := primitive.ObjectIDFromHex(ctx.UserValue("stream").(string))
		if err != nil {
			ctx.SetBodyString("Not Found")
			ctx.SetStatusCode(fasthttp.StatusNotFound)
			return
		}

		auth := ctx.QueryArgs().Peek("auth")
		if len(auth) == 0 {
			ctx.SetBodyString("Forbidden")
			ctx.SetStatusCode(fasthttp.StatusForbidden)
			return
		}

		pl := structures.JwtWatchStream{}
		if err := structures.DecodeJwt(&pl, gCtx.Config().Auth.JwtToken, utils.B2S(auth)); err != nil {
			ctx.SetBodyString("Forbidden")
			ctx.SetStatusCode(fasthttp.StatusForbidden)
			return
		}

		if pl.StreamID != streamID {
			ctx.SetBodyString("Forbidden")
			ctx.SetStatusCode(fasthttp.StatusForbidden)
			return
		}

		ids := []primitive.ObjectID{pl.ChannelID}
		if !pl.UserID.IsZero() {
			ids = append(ids, pl.UserID)
		}

		// check the user's memberships
		users, errs := loader.LoadAll(ids)
		for _, err := range errs {
			if err != nil {
				logrus.Error("unable to check auth", err)
				ctx.SetBodyString("Internal Server Error")
				ctx.SetStatusCode(fasthttp.StatusInternalServerError)
				return
			}
		}

		channel := users[0]
		if !channel.Channel.Public {
			if pl.UserID.IsZero() {
				ctx.SetBodyString("Forbidden")
				ctx.SetStatusCode(fasthttp.StatusForbidden)
				return
			}

			user := users[1]
			if user.Role < structures.GlobalRoleStaff {
				found := false
				for _, v := range user.Memberships {
					if v.ChannelID == channel.ID {
						if v.Role >= structures.ChannelRoleViewer {
							found = true
						}
						break
					}
				}
				if !found {
					ctx.SetBodyString("Forbidden")
					ctx.SetStatusCode(fasthttp.StatusForbidden)
					return
				}
			}
		}

		// we need to get the ip of the pod serving these segment files
		val, err := gCtx.Inst().Redis.Get(ctx, fmt.Sprintf("stream:%s:variants", streamID.Hex()))
		if err != nil {
			if err == redis.Nil {
				ctx.SetBodyString("Not Found")
				ctx.SetStatusCode(fasthttp.StatusNotFound)
				return
			}
			logrus.Error("unable to fetch playlist", err)
			ctx.SetBodyString("Internal Server Error")
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}

		variants := []structures.JwtMuxerPayloadVariant{}

		if err := json.UnmarshalFromString(val.(string), &variants); err != nil {
			logrus.Error("unable to fetch playlist", err)
			ctx.SetBodyString("Internal Server Error")
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}

		buf := bytes.NewBuffer(nil)
		_, _ = buf.WriteString("#EXTM3U\n")
		for _, v := range variants {
			_, _ = buf.WriteString(fmt.Sprintf("#EXT-X-STREAM-INF:BANDWIDTH=%d,AVERAGE-BANDWIDTH=%d,RESOLUTION=%dx%d,FRAME-RATE=%d,CODECS=\"%s\",NAME=\"%s\"\n", v.Bitrate, v.Bitrate, v.Width, v.Height, v.FPS, v.Codecs, v.Name))
			_, _ = buf.WriteString(fmt.Sprintf("%s/playlist.m3u8?auth=%s\n", v.Name, auth))
		}

		ctx.Response.Header.SetContentType("application/x-mpegURL")
		ctx.SetBodyString(buf.String())
	})

	router.GET("/{stream}/{variant}/playlist.m3u8", func(ctx *fasthttp.RequestCtx) {
		streamID, err := primitive.ObjectIDFromHex(ctx.UserValue("stream").(string))
		if err != nil {
			ctx.SetBodyString("Not Found")
			ctx.SetStatusCode(fasthttp.StatusNotFound)
			return
		}

		auth := ctx.QueryArgs().Peek("auth")
		if len(auth) == 0 {
			ctx.SetBodyString("Forbidden")
			ctx.SetStatusCode(fasthttp.StatusForbidden)
			return
		}

		pl := structures.JwtWatchStream{}
		if err := structures.DecodeJwt(&pl, gCtx.Config().Auth.JwtToken, utils.B2S(auth)); err != nil {
			ctx.SetBodyString("Forbidden")
			ctx.SetStatusCode(fasthttp.StatusForbidden)
			return
		}

		if pl.StreamID != streamID {
			ctx.SetBodyString("Forbidden")
			ctx.SetStatusCode(fasthttp.StatusForbidden)
			return
		}

		ids := []primitive.ObjectID{pl.ChannelID}
		if !pl.UserID.IsZero() {
			ids = append(ids, pl.UserID)
		}

		// check the user's memberships
		users, errs := loader.LoadAll(ids)
		for _, err := range errs {
			if err != nil {
				logrus.Error("unable to check auth", err)
				ctx.SetBodyString("Internal Server Error")
				ctx.SetStatusCode(fasthttp.StatusInternalServerError)
				return
			}
		}

		channel := users[0]
		if !channel.Channel.Public {
			if pl.UserID.IsZero() {
				ctx.SetBodyString("Forbidden")
				ctx.SetStatusCode(fasthttp.StatusForbidden)
				return
			}

			user := users[1]
			if user.Role < structures.GlobalRoleStaff || user.MemberRole(channel.ID) < structures.ChannelRoleViewer {
				ctx.SetBodyString("Forbidden")
				ctx.SetStatusCode(fasthttp.StatusForbidden)
				return
			}
		}

		h := sha256.New()
		_, _ = h.Write(auth)
		key := hex.EncodeToString(h.Sum(nil))

		if _, err := gCtx.Inst().Mongo.Collection(mongo.CollectionNameCountDocuments).UpdateOne(ctx, bson.M{
			"key":   key,
			"group": pl.ChannelID,
			"type":  structures.CountDocumentTypeViewer,
		}, bson.M{
			"$set": bson.M{
				"expiry": time.Now().Add(time.Second * 15),
			},
			"$setOnInsert": bson.M{
				"key":   key,
				"group": pl.ChannelID,
				"type":  structures.CountDocumentTypeViewer,
			},
		}, options.Update().SetUpsert(true)); err != nil {
			logrus.Error("could not upsert: ", err)
		}

		variant := ctx.UserValue("variant").(string)

		// we need to get the ip of the pod serving these segment files
		val, err := gCtx.Inst().Redis.Get(ctx, fmt.Sprintf("live-playlists:%s:%s", streamID.Hex(), variant))
		if err != nil {
			if err == redis.Nil {
				ctx.SetBodyString("Not Found")
				ctx.SetStatusCode(fasthttp.StatusNotFound)
				return
			}
			logrus.Error("unable to fetch playlist", err)
			ctx.SetBodyString("Internal Server Error")
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}

		ctx.Response.Header.SetContentType("application/x-mpegURL")
		ctx.SetBodyString(val.(string))
	})

	router.GET("/{stream}/{variant}/{segment}.ts", func(ctx *fasthttp.RequestCtx) {
		streamID, err := primitive.ObjectIDFromHex(ctx.UserValue("stream").(string))
		if err != nil {
			ctx.SetBodyString("Not Found")
			ctx.SetStatusCode(fasthttp.StatusNotFound)
			return
		}

		variant := ctx.UserValue("variant").(string)

		// we need to get the ip of the pod serving these segment files
		val, err := gCtx.Inst().Redis.Get(ctx, fmt.Sprintf("live-playlists:%s:%s:ip", streamID.Hex(), variant))
		if err != nil {
			if err == redis.Nil {
				ctx.SetBodyString("Not Found")
				ctx.SetStatusCode(fasthttp.StatusNotFound)
				return
			}
			logrus.Error("unable to fetch ip", err)
			ctx.SetBodyString("Internal Server Error")
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}

		ctx.Request.Header.Del("connection")

		resp, err := http.DefaultClient.Get(fmt.Sprintf("http://%s%s", val.(string), ctx.Path()))
		if err != nil {
			logrus.Error(err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}

		for k, v := range resp.Header {
			for _, h := range v {
				ctx.Response.Header.Add(k, h)
			}
		}

		ctx.Response.ImmediateHeaderFlush = true
		ctx.SetStatusCode(resp.StatusCode)
		ctx.SetBodyStream(resp.Body, -1)
	})

	go func() {
		if err := srv.ListenAndServe(gCtx.Config().Edge.Bind); err != nil {
			logrus.Fatal("failed to bind edge: ", err)
		}
		close(done)
	}()

	go func() {
		<-gCtx.Done()
		_ = srv.Shutdown()
	}()

	return done
}
