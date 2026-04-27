import winsound


def finished_sound():
    sound_path = r"C:\Users\gouldd\OneDrive - Sutter Health\_HomeDrive\SQL\LawsonProject\Python_Dev\Finished_DataPull.wav"
    try:
        winsound.PlaySound(sound_path, winsound.SND_FILENAME)
    except Exception:
        winsound.MessageBeep() # Fallback if file is missing
def main():
    # Put all the stuff you want to happen ONLY when running this file directly
    # e.g., the Get_Init_Data_CSV(), the file dialogs, etc.
    print("Running Refresh Checker as a standalone tool...")
    # ... code ...

if __name__ == "__main__":
    main()