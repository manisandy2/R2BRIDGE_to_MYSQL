from main import app
from mangum import Mangum

handler = Mangum(app)

async def on_fetch(request):
    return await handler(request)
