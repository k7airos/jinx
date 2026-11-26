package dispatcher

import "testing"

func TestActualBackend(t *testing.T) {
	for _, tc := range []struct {
		r    HermesResult
		want string
	}{
		{HermesResult{SessionID: "alma:abc"}, "Alma"},
		{HermesResult{SessionID: "local-abc"}, "Hermes"},
		{HermesResult{Backend: "Hermes", Fallback: true, SessionID: "local-abc"}, "Hermes"},
	} {
		if got := tc.r.actualBackend(); got != tc.want {
			t.Fatalf("%s != %s", got, tc.want)
		}
	}
	r, err := parseHermesOutput([]byte(`{"type":"result","text":"ok","session_id":"local-test","exit_code":0,"backend":"Hermes","fallback":true}`))
	if err != nil || !r.Fallback || r.actualBackend() != "Hermes" {
		t.Fatalf("lost backend metadata: %+v %v", r, err)
	}
}
