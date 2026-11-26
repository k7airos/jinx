package dispatcher

import (
	"bufio"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"jinx/internal/contracts"
)

const (
	historyKeepDays  = 2   // today plus yesterday, so a search right after midnight still works
	historyMaxRunes  = 500 // stored per message
	recallMaxResults = 6
	recallMaxRunes   = 200
	// A match needs either an uncommon ASCII word (a product, model or API name) or enough
	// rare tokens in common; two everyday bigrams such as "我们"/"什么" must not qualify.
	recallMinScore = 7.0
)

type historyRecord struct {
	ID         string `json:"id"`
	Timestamp  int64  `json:"ts"`
	SenderName string `json:"sender"`
	Type       int    `json:"type"`
	Content    string `json:"content"`
	IsEcho     bool   `json:"echo,omitempty"`
}

// historyStore keeps each routed group's recent messages on local disk so the dispatcher can
// look up something said long before the 25-message context window. It only stores what the
// dispatcher already received from the relay; it never reads any other source.
// All methods are safe on a nil receiver.
type historyStore struct {
	dir       string
	mu        sync.Mutex
	lastPrune string
}

func newHistoryStore(dir string) *historyStore { return &historyStore{dir: dir} }

func (h *historyStore) path(groupID, day string) string {
	return filepath.Join(h.dir, contracts.SanitizeProfileSuffix(groupID)+"_"+day+".jsonl")
}

func (h *historyStore) Append(msg RelayMessage) {
	if h == nil || !contracts.ValidContainerID(msg.GroupID) {
		return
	}
	content := strings.TrimSpace(msg.Content)
	if content == "" {
		return
	}
	if utf8.RuneCountInString(content) > historyMaxRunes {
		content = string([]rune(content)[:historyMaxRunes])
	}
	line, err := json.Marshal(historyRecord{ID: msg.ID, Timestamp: msg.Timestamp, SenderName: msg.SenderName, Type: msg.Type, Content: content, IsEcho: msg.IsEcho})
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := os.MkdirAll(h.dir, 0700); err != nil {
		return
	}
	f, err := os.OpenFile(h.path(msg.GroupID, time.UnixMilli(msg.Timestamp).Format("2006-01-02")), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return
	}
	_, _ = f.Write(append(line, '\n'))
	_ = f.Close()
	h.pruneLocked(time.Now())
}

// pruneLocked deletes day files older than historyKeepDays, at most once per day.
func (h *historyStore) pruneLocked(now time.Time) {
	today := now.Format("2006-01-02")
	if h.lastPrune == today {
		return
	}
	h.lastPrune = today
	cutoff := now.AddDate(0, 0, -(historyKeepDays - 1)).Format("2006-01-02")
	entries, err := os.ReadDir(h.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".jsonl") || len(name) < len("_2006-01-02.jsonl") {
			continue
		}
		day := name[len(name)-len("2006-01-02.jsonl") : len(name)-len(".jsonl")]
		if day < cutoff {
			_ = os.Remove(filepath.Join(h.dir, name))
		}
	}
}

func (h *historyStore) load(groupID, day string) []historyRecord {
	f, err := os.Open(h.path(groupID, day))
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []historyRecord
	scanner := bufio.NewScanner(io.LimitReader(f, 16<<20))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		var r historyRecord
		if json.Unmarshal(scanner.Bytes(), &r) == nil && r.Content != "" {
			out = append(out, r)
		}
	}
	return out
}

// Search returns up to recallMaxResults of today's earlier messages that share rare keywords
// with the given query texts, oldest first. Messages in exclude (already in the prompt) and
// the bot's own replies are skipped; ignore lists words that never count (the bot's own names).
func (h *historyStore) Search(groupID string, queries []string, exclude map[string]bool, now time.Time, ignore []string) []RelayMessage {
	if h == nil || !contracts.ValidContainerID(groupID) {
		return nil
	}
	query := tokenSet(strings.Join(queries, "\n"))
	for _, name := range ignore {
		delete(query, strings.ToLower(strings.TrimSpace(name)))
	}
	if len(query) == 0 {
		return nil
	}
	h.mu.Lock()
	records := h.load(groupID, now.Format("2006-01-02"))
	h.mu.Unlock()
	if len(records) == 0 {
		return nil
	}
	tokens := make([]map[string]bool, len(records))
	df := map[string]int{}
	for i, r := range records {
		tokens[i] = tokenSet(r.Content)
		for t := range tokens[i] {
			df[t]++
		}
	}
	type hit struct {
		idx   int
		score float64
	}
	var hits []hit
	n := float64(len(records))
	for i, r := range records {
		if r.IsEcho || exclude[r.ID] {
			continue
		}
		score, strong := 0.0, false
		for t := range query {
			if !tokens[i][t] {
				continue
			}
			score += math.Log(1 + n/float64(df[t]))
			if isASCIIWord(t) && len(t) >= 4 {
				strong = true
			}
		}
		if strong || score >= recallMinScore {
			hits = append(hits, hit{i, score})
		}
	}
	sort.SliceStable(hits, func(a, b int) bool { return hits[a].score > hits[b].score })
	if len(hits) > recallMaxResults {
		hits = hits[:recallMaxResults]
	}
	sort.Slice(hits, func(a, b int) bool { return hits[a].idx < hits[b].idx })
	out := make([]RelayMessage, 0, len(hits))
	for _, ht := range hits {
		r := records[ht.idx]
		content := r.Content
		if utf8.RuneCountInString(content) > recallMaxRunes {
			content = string([]rune(content)[:recallMaxRunes]) + "…"
		}
		out = append(out, RelayMessage{ID: r.ID, SenderName: r.SenderName, Type: r.Type, Content: content, Timestamp: r.Timestamp})
	}
	return out
}

func isASCIIWord(s string) bool {
	for _, r := range s {
		if r > unicode.MaxASCII {
			return false
		}
	}
	return s != ""
}

var recallStopASCII = map[string]bool{"the": true, "and": true, "for": true, "you": true, "are": true, "this": true, "that": true, "with": true, "have": true, "from": true, "not": true, "but": true, "can": true, "was": true}

// Everyday bigrams that carry no topic; IDF already discounts them, this just removes the worst.
var recallStopHan = map[string]bool{
	"我们": true, "你们": true, "他们": true, "什么": true, "怎么": true, "可以": true, "这个": true, "那个": true, "没有": true,
	"不是": true, "就是": true, "还是": true, "一个": true, "但是": true, "因为": true, "所以": true, "如果": true, "已经": true,
	"现在": true, "知道": true, "觉得": true, "应该": true, "可能": true, "时候": true, "今天": true, "大家": true, "哈哈": true,
	"不用": true, "为啥": true, "是不": true, "有没": true, "一下": true, "感觉": true, "看看": true, "东西": true,
}

// tokenSet splits text into lowercase ASCII words (3+ chars) and Han bigrams. No segmenter is
// needed: bigram overlap is enough to find "the same topic" inside one day of a group's chat.
func tokenSet(text string) map[string]bool {
	out := map[string]bool{}
	var word, han []rune
	flushWord := func() {
		w := strings.Trim(string(word), ".-_")
		word = word[:0]
		if len(w) >= 3 && !recallStopASCII[w] {
			out[w] = true
		}
	}
	flushHan := func() {
		for i := 0; i+1 < len(han); i++ {
			if bg := string(han[i : i+2]); !recallStopHan[bg] {
				out[bg] = true
			}
		}
		han = han[:0]
	}
	for _, r := range strings.ToLower(text) {
		switch {
		case unicode.Is(unicode.Han, r):
			flushWord()
			han = append(han, r)
		case r <= unicode.MaxASCII && (unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.' || r == '_' || r == '-'):
			flushHan()
			word = append(word, r)
		default:
			flushWord()
			flushHan()
		}
	}
	flushWord()
	flushHan()
	return out
}
