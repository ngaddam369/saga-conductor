package dashboard

import (
	"fmt"
	"net/http"
)

// SSEHandler returns an http.HandlerFunc that streams saga update events to
// the browser as Server-Sent Events. Each event is named "update" and carries
// a JSON-encoded sagaEvent payload. The handler blocks until the client
// disconnects or the request context is cancelled.
func (b *Broadcaster) SSEHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		// Prevent nginx/proxies from buffering the stream.
		w.Header().Set("X-Accel-Buffering", "no")

		ch, unsub := b.subscribe()
		defer unsub()

		// Immediately send a comment so the browser knows the stream is live.
		if _, err := fmt.Fprint(w, ": connected\n\n"); err != nil {
			return
		}
		flusher.Flush()

		for {
			select {
			case <-r.Context().Done():
				return
			case data := <-ch:
				if _, err := fmt.Fprintf(w, "event: update\ndata: %s\n\n", data); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}
}

// PageHandler returns an http.HandlerFunc that serves the dashboard HTML page.
func PageHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if _, err := fmt.Fprint(w, dashboardHTML); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Saga Conductor</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,sans-serif;background:#f0f2f5;padding:20px;color:#1a1a2e}
header{display:flex;align-items:center;justify-content:space-between;margin-bottom:20px}
h1{font-size:1.3rem;font-weight:700;letter-spacing:-.3px}
#conn{font-size:.78rem;padding:3px 10px;border-radius:12px;font-weight:600}
.ok{background:#dcfce7;color:#166534}.err{background:#fee2e2;color:#991b1b}.wait{background:#f3f4f6;color:#6b7280}
#sagas{display:grid;gap:12px}
.card{background:#fff;border-radius:10px;padding:16px 18px;box-shadow:0 1px 4px rgba(0,0,0,.08)}
.card-header{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:12px;gap:8px}
.saga-name{font-weight:600;font-size:.97rem}
.saga-id{font-size:.7rem;color:#888;font-family:monospace;margin-top:2px}
.steps{display:flex;flex-wrap:wrap;gap:8px}
.step{display:flex;flex-direction:column;padding:6px 10px;border-radius:6px;font-size:.78rem;min-width:80px}
.step-name{font-weight:600;margin-bottom:3px;white-space:nowrap}
.step-err{font-size:.7rem;margin-top:3px;word-break:break-word;max-width:200px}
.badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.72rem;font-weight:700;white-space:nowrap}
.PENDING{background:#f3f4f6;color:#6b7280}
.RUNNING{background:#dbeafe;color:#1d4ed8}
.SUCCEEDED,.COMPLETED,.COMPENSATED{background:#dcfce7;color:#166534}
.FAILED,.COMPENSATION_FAILED{background:#fee2e2;color:#991b1b}
.COMPENSATING{background:#fef3c7;color:#92400e}
.ABORTED{background:#f3f4f6;color:#374151}
#empty{text-align:center;color:#9ca3af;margin-top:60px;font-size:.9rem}
</style>
</head>
<body>
<header>
  <h1>Saga Conductor</h1>
  <span id="conn" class="wait">Connecting…</span>
</header>
<div id="sagas"></div>
<div id="empty">No sagas yet. Start a saga to see it here.</div>
<script>
const sagas={};
const connEl=document.getElementById('conn');
const sagasEl=document.getElementById('sagas');
const emptyEl=document.getElementById('empty');

const es=new EventSource('/dashboard/events');
es.onopen=()=>{connEl.textContent='Connected';connEl.className='ok'};
es.onerror=()=>{connEl.textContent='Disconnected — retrying…';connEl.className='err'};
es.addEventListener('update',e=>{
  const s=JSON.parse(e.data);
  sagas[s.id]=s;
  render();
});

function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;')}
function badge(st){return '<span class="badge '+esc(st)+'">'+esc(st)+'</span>'}

function renderSaga(s){
  const steps=s.steps.map(t=>'<div class="step '+esc(t.status)+'">'
    +'<span class="step-name">'+esc(t.name)+'</span>'
    +badge(t.status)
    +(t.error?'<span class="step-err">'+esc(t.error)+'</span>':'')
    +'</div>').join('');
  return '<div class="card">'
    +'<div class="card-header">'
    +'<div><div class="saga-name">'+esc(s.name)+'</div>'
    +'<div class="saga-id">'+esc(s.id)+'</div></div>'
    +badge(s.status)+'</div>'
    +'<div class="steps">'+steps+'</div>'
    +'</div>';
}

function render(){
  const ids=Object.keys(sagas);
  emptyEl.style.display=ids.length?'none':'';
  sagasEl.innerHTML=ids.slice().reverse().map(id=>renderSaga(sagas[id])).join('');
}
</script>
</body>
</html>`
