package com.datasphere.metaRepository;

import java.util.concurrent.*;
import java.io.*;

public interface RemoteCall<T> extends Callable<T>, Serializable
{
}
