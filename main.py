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
        print("\n")

    def handle_selection(self) -> None:
        match self.selected:
            case "📂 Daten herunterladen":
                
                print("Daten anzeigen")
            case "Datenbank erstellen":
                
                print("Analyse starten")
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
