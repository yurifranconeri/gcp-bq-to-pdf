import io
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

def render_pdf(row):
    pdf_buffer = io.BytesIO()
    c = canvas.Canvas(pdf_buffer, pagesize=letter)
    c.drawString(100, 730, f"Name: {row.nome}")
    c.save()
    pdf_buffer.seek(0)
    return pdf_buffer.getvalue()