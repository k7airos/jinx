package ingest

import "testing"

func TestQuoteOriginalSurvivesParsing(t *testing.T) {
	raw := `<msg><appmsg><type>57</type><title>@JINX 仿写</title><refermsg><type>1</type><displayname>原作者</displayname><content><![CDATA[入夜起了风，引用原文]]></content></refermsg></appmsg></msg>`
	d := ParseMessageDetails(49, raw)
	if d.Quote == nil || d.Quote.Author != "原作者" || d.Quote.Content != "入夜起了风，引用原文" {
		t.Fatalf("missing quote: %+v", d)
	}
	if got := SummarizeContent(49, raw); got != "[引用消息] @JINX 仿写" {
		t.Fatal(got)
	}
	if ParseMessageDetails(49, `<msg><appmsg>`).Quote != nil {
		t.Fatal("malformed quote invented")
	}
}
