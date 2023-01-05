from pydantic import BaseModel


class SaveResponse(BaseModel) :
	fingerprint: int
