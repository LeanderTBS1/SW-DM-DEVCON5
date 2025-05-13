import questionary

class Menu:
    def __init__(self):
        self.selected = None
        self.running = True

    def display_menu(self) -> None:
        self.selected = questionary.select(
            "Was möchtest du tun?",
            choices=[
                "📂 Daten herunterladen",
                "Datenbank erstellen",
                "❌ Beenden"
            ]
        ).ask()

    def handle_selection(self) -> None:
        match self.selected:
            case "📂 Daten anzeigen":
                print("Daten anzeigen")
                # Hier kannst du den Code zum Anzeigen der Daten einfügen
            case "🧮 Analyse starten":
                print("Analyse starten")
                # Hier kannst du den Code zur Analyse einfügen
            case "❌ Beenden":
                print("Beenden")
                self.running = False
            case _:
                print("Ungültige Auswahl. Bitte wähle eine gültige Option.")

    def main(self) -> None:
        while self.running:
            self.display_menu()
            self.handle_selection()

if __name__ == "__main__":
    menu = Menu()
    menu.main()
