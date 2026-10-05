// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Like grpc.ClientStream, a stream permits one sender and one receiver. CloseSend
// belongs to the sender. Each direction buffers one detached message; cancellation
// releases blocked operations. Half-close does not cancel the response stream.
type substituteStream struct {
	call        *substituteCall
	requests    chan proto.Message
	responses   chan proto.Message
	done        chan struct{}
	err         error // Published by closing responses and done.
	sendClosed  bool
	metadataMu  sync.Mutex
	header      metadata.MD
	trailer     metadata.MD
	headerSent  bool
	headerErr   error
	headerReady chan struct{}
}

type substituteServerStream struct{ *substituteStream }

var (
	_ grpc.ClientStream = (*substituteStream)(nil)
	_ grpc.ServerStream = (*substituteServerStream)(nil)
)

func newSubstituteStream(call *substituteCall) *substituteStream {
	return &substituteStream{
		call: call, requests: make(chan proto.Message, 1), responses: make(chan proto.Message, 1),
		done: make(chan struct{}), headerReady: make(chan struct{}),
	}
}

func (s *substituteStream) Context() context.Context { return s.call.ctx }

func (s *substituteStream) Header() (metadata.MD, error) {
	select {
	case <-s.headerReady:
	default:
		select {
		case <-s.headerReady:
		case <-s.Context().Done():
			select {
			case <-s.headerReady:
			default:
				return nil, substituteStatus(s.Context().Err())
			}
		}
	}
	s.metadataMu.Lock()
	defer s.metadataMu.Unlock()
	return s.header.Copy(), s.headerErr
}

func (s *substituteStream) Trailer() metadata.MD {
	s.metadataMu.Lock()
	defer s.metadataMu.Unlock()
	return s.trailer.Copy()
}

func (s *substituteStream) CloseSend() error {
	if !s.sendClosed {
		s.sendClosed = true
		close(s.requests)
	}
	return nil
}

func (s *substituteStream) SendMsg(value any) error {
	if s.sendClosed {
		return status.Error(codes.Internal, "SendMsg called after CloseSend")
	}
	select {
	case <-s.done:
		return io.EOF
	default:
	}
	if err := s.Context().Err(); err != nil {
		return substituteStatus(err)
	}
	message, err := cloneSubstituteMessage(value)
	if err != nil {
		return err
	}
	select {
	case s.requests <- message:
		return nil
	case <-s.done:
		return io.EOF
	case <-s.Context().Done():
		return substituteStatus(s.Context().Err())
	}
}

func (s *substituteStream) RecvMsg(value any) error {
	// Prefer queued responses, including when the handler has already returned
	// and canceled its context. Completion never discards an accepted response.
	select {
	case message, ok := <-s.responses:
		return s.receive(value, message, ok)
	default:
	}
	select {
	case message, ok := <-s.responses:
		return s.receive(value, message, ok)
	case <-s.Context().Done():
		select {
		case message, ok := <-s.responses:
			return s.receive(value, message, ok)
		default:
			return substituteStatus(s.Context().Err())
		}
	}
}

func (s *substituteStream) receive(value any, message proto.Message, ok bool) error {
	if !ok {
		if s.err != nil {
			return s.err
		}
		return io.EOF
	}
	return copySubstituteMessage(value, message)
}

func (s *substituteStream) publishHeader(err error) {
	s.metadataMu.Lock()
	defer s.metadataMu.Unlock()
	if !s.headerSent {
		s.headerSent = true
		s.headerErr = err
		close(s.headerReady)
	}
}

func (s *substituteServerStream) SetHeader(md metadata.MD) error {
	s.metadataMu.Lock()
	defer s.metadataMu.Unlock()
	if s.headerSent {
		return status.Error(codes.Internal, "substitute stream headers already sent")
	}
	s.header = metadata.Join(s.header, md)
	return nil
}

func (s *substituteServerStream) SendHeader(md metadata.MD) error {
	if err := s.SetHeader(md); err != nil {
		return err
	}
	s.publishHeader(nil)
	return nil
}

func (s *substituteServerStream) SetTrailer(md metadata.MD) {
	s.metadataMu.Lock()
	defer s.metadataMu.Unlock()
	s.trailer = metadata.Join(s.trailer, md)
}

func (s *substituteServerStream) SendMsg(value any) error {
	if err := s.Context().Err(); err != nil {
		return substituteStatus(err)
	}
	message, err := cloneSubstituteMessage(value)
	if err != nil {
		return err
	}
	s.publishHeader(nil)
	select {
	case s.responses <- message:
		return nil
	case <-s.Context().Done():
		return substituteStatus(s.Context().Err())
	}
}

func (s *substituteServerStream) RecvMsg(value any) error {
	if err := s.Context().Err(); err != nil {
		return substituteStatus(err)
	}
	select {
	case message, ok := <-s.requests:
		if !ok {
			return io.EOF
		}
		return copySubstituteMessage(value, message)
	case <-s.Context().Done():
		return substituteStatus(s.Context().Err())
	}
}
