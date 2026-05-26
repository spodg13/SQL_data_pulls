import os
import winsound

# 1. Define your dictionary of sounds
# You can place this at the top of your script as a constant
SOUND_LIBRARY = {
    "finished": r"C:\Users\gouldd\OneDrive - Sutter Health\_HomeDrive\SQL\LawsonProject\Python_Dev\Finished_DataPull.wav",
    "final": r"C:\Users\gouldd\OneDrive - Sutter Health\_HomeDrive\SQL\LawsonProject\Python_Dev\Finished_2.wav",
    "alert": r"C:\Users\gouldd\OneDrive - Sutter Health\_HomeDrive\SQL\LawsonProject\Python_Dev\Alert_Attention.wav"
}

# 2. Modify the function to accept a variable
def play_sound(sound_name="finished"):
    """
    Plays a sound from the SOUND_LIBRARY based on the provided key name.
    Defaults to 'finished' if no name is passed.
    """
    # Use .get() to safely grab the path, or None if the key doesn't exist
    sound_path = SOUND_LIBRARY.get(sound_name)
    
    # Check if the path exists in our dictionary AND on the actual hard drive
    if sound_path and os.path.exists(sound_path):
        try:
            winsound.PlaySound(sound_path, winsound.SND_FILENAME)
        except Exception:
            winsound.MessageBeep()  # Fallback if winsound fails
    else:
        # Fallback if the dictionary key is wrong or the file is missing
        print(f"Warning: Sound '{sound_name}' not found or file missing. Playing fallback beep.")
        winsound.MessageBeep()

def main():
    print("Running Refresh Checker as a standalone tool...")
    
    # ... your data pull code here ...
    
    # 3. Call your function and pass the specific sound you want
    play_sound("finished") 

if __name__ == "__main__":
    main()