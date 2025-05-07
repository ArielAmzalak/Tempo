# launcher.py
import sys, os
from streamlit.web import cli as stcli

if __name__ == "__main__":
    # Se estivermos empacotados no PyInstaller, extrai em _MEIPASS
    if getattr(sys, "frozen", False):
        base_path = sys._MEIPASS
    else:
        base_path = os.path.dirname(os.path.abspath(__file__))

    app_file = os.path.join(base_path, "app.py")
    if not os.path.exists(app_file):
        print(f"❌ Arquivo não encontrado: {app_file}")
        sys.exit(1)

    # Monta a chamada ao Streamlit, já em headless e desativando XSRF
    sys.argv = [
        "streamlit", "run", app_file,
        "--server.headless=true",
        "--server.enableXsrfProtection=false"  # para evitar o warning de CORS
    ]
    sys.exit(stcli.main())
