from avrofastapi.schema import AvroSchema
from kh_common.server import Request, ServerApp

from models import SaveResponse
from schema_repository import SchemaRepository


app = ServerApp(
	auth = False,
	allowed_hosts = [
		'localhost',
		'127.0.0.1',
		'*.fuzz.ly',
		'fuzz.ly',
	],
	allowed_origins = [
		'localhost',
		'127.0.0.1',
		'dev.fuzz.ly',
		'fuzz.ly',
	],
)
repo: SchemaRepository = SchemaRepository()


@app.on_event('shutdown')
async def shutdown() :
	repo.close()


@app.get('/v1/schema/{fingerprint}')
async def v1Schema(fingerprint: str) :
	return await repo.getSchema(fingerprint)


@app.post('/v1/schema', response_model=SaveResponse)
async def v1SaveSchema(req: Request) :
	return SaveResponse(
		fingerprint=await repo.addSchema(await req.json()),
	)


if __name__ == '__main__' :
	from uvicorn.main import run
	run(app, host='0.0.0.0', port=5007)
