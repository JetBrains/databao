def in_jupyter_kernel() -> bool:
    """
    Return True when running inside a Jupyter/IPython kernel (ipykernel).

    Notes:
    - This is typically True in Jupyter Notebook, JupyterLab, VS Code notebooks, Google Colab.
    - This is typically False in regular Python scripts and in terminal IPython.
    - The check is best-effort: if IPython isn't available or behaves unexpectedly,
      this returns False.
    """
    try:
        from IPython import get_ipython  # type: ignore

        ip = get_ipython()
        if ip is None:
            return False

        # In ipykernel, the config usually contains this key.
        return "IPKernelApp" in getattr(ip, "config", {})
    except Exception:
        return False
