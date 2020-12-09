// Copyright (C) 2017-2020  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Free Software licenses or any of the Open Source
// Initiative approved licenses and Convey the resulting work. Corresponding
// source of such a combination shall include the source code for all other
// software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.
// See https://www.nexedi.com/licensing for rationale and options.

package neo

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"

	"lab.nexedi.com/kirr/go123/xerr"

	"lab.nexedi.com/kirr/neo/go/zodb"
	"lab.nexedi.com/kirr/neo/go/internal/log"
)

// lclose closes c and logs closing error if there was any.
// the error is otherwise ignored
func lclose(ctx context.Context, c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Error(ctx, err)
	}
}

// at2Before converts at to before for ZODB load semantics taking edge cases into account.
//
// For most values it is
//
//	before = at + 1		; at < ∞
//
// but at ∞ (zodb.TidMax) it is just
//
//	before = at		; at = ∞
func at2Before(at zodb.Tid) (before zodb.Tid) {
	if at < zodb.TidMax {
		return at + 1
	} else {
		// XXX do we need to care here also for at > zodb.TidMax (zodb.Tid is currently unsigned)
		return zodb.TidMax
	}
}

// before2At is the reverse function to at2Before
func before2At(before zodb.Tid) (at zodb.Tid) {
	if before < zodb.TidMax {
		// XXX before = 0 ?
		return before - 1
	} else {
		// XXX before > zodb.TidMax (same as in at2Before) ?
		return zodb.TidMax
	}
}


// tlsForSSL builds tls.Config from ca/cert/key files that should be interoperable with NEO/py.
//
// see https://lab.nexedi.com/nexedi/neoppod/blob/v1.12-61-gc1c26894/neo/lib/app.py#L74-90
func tlsForSSL(ca, cert, key string) (_ *tls.Config, err error) {
	defer xerr.Contextf(&err, "tls setup")

	caData, err := ioutil.ReadFile(ca)
	if err != nil {
		return nil, err
	}

	CA := x509.NewCertPool()
	ok := CA.AppendCertsFromPEM(caData)
	if !ok {
		return nil, fmt.Errorf("invalid CA")
	}

	crt, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}


	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{crt}, // (cert, key) as loaded
		RootCAs: CA,                          // (ca,) as loaded

		// a server also verifies cient (but also see verifyPeerCert below)
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs: CA,

		PreferServerCipherSuites: true,
		MinVersion:               tls.VersionTLS12, // only accept TLS >= 1.2
	}

	// tls docs say we should parse Certificate[0] into Leaf ourselves
	leaf, err := x509.ParseCertificate(crt.Certificate[0])
	if err != nil {
		return nil, err
	}
	crt.Leaf = leaf

	// NEO/py does not verify CommonName (ssl.check_hostname=False implicitly).
	// Match that behaviour with custom VerifyPeerCertificate because Go
	// does not provide functionality to skip only CN verification out of the box.
	// https://github.com/golang/go/issues/21971#issuecomment-332693931
	// https://stackoverflow.com/questions/44295820
	verifyPeerCert := func(rawCerts [][]byte, _ [][]*x509.Certificate) (err error) {
		defer xerr.Contextf(&err, "verify peer cert")

		certv := []*x509.Certificate{}
		for _, certData := range rawCerts {
			cert, err := x509.ParseCertificate(certData)
			if err != nil {
				return err
			}
			certv = append(certv, cert)
		}

		vopt := x509.VerifyOptions{
			DNSName:       "",                // means "don't verify name"
			Roots:         tlsCfg.RootCAs,
			Intermediates: x509.NewCertPool(),
		}
		for _, cert := range certv[1:] {
			vopt.Intermediates.AddCert(cert)
		}

		_, err = certv[0].Verify(vopt)
		return err
	}
	tlsCfg.InsecureSkipVerify = true // disables all verifications including for ServerName
	tlsCfg.VerifyPeerCertificate = verifyPeerCert

	return tlsCfg, nil
}
