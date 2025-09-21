import json, logging, sys
from datetime import datetime, timezone
class JsonFormatter(logging.Formatter):
    def format(self, record):
        p={"ts":datetime.now(timezone.utc).isoformat(),
           "level":record.levelname,"logger":record.name,"msg":record.getMessage()}
        if record.exc_info: p["exc_info"]=self.formatException(record.exc_info)
        return json.dumps(p, ensure_ascii=False)
def configure_logging(level=logging.INFO):
    h=logging.StreamHandler(sys.stdout); h.setFormatter(JsonFormatter())
    root=logging.getLogger(); root.handlers.clear(); root.addHandler(h); root.setLevel(level)
